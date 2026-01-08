use calamine::{Data, DataType, Reader, Xlsx};
use chrono::NaiveDateTime;
use eframe::egui;
use rfd::FileDialog;
use rust_xlsxwriter::Workbook;
use std::collections::{BTreeSet, HashSet};
use std::error::Error;
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread;

const NAME_COL: &str = "@Name( )";
const TEMPERATURE_COL: &str = "Тemperature";

// Структура данных
#[derive(Debug, Clone)]
struct WellRecord {
    well_name: String,
    date: Option<NaiveDateTime>,
    pd_liq: Option<f64>,
    pd_oil: Option<f64>,
    temperature: Option<f64>,
    year_sheet: i32,
}

// Типы сообщений от воркера к UI
enum LoaderMessage {
    Progress(f32, f32, String),
    Loaded((Vec<WellRecord>, Vec<i32>, Vec<String>)),
    Saved(String),
    Error(String),
}

struct WellDataApp {
    raw_data: Vec<WellRecord>,
    available_years: Vec<i32>,
    unique_wells: Vec<String>,

    source_file_path: Option<String>,
    selected_start_year: Option<i32>,
    selected_wells: HashSet<String>,

    search_query: String,

    status_message: String,
    is_loading: bool,
    progress_global: f32,
    progress_local: f32,

    rx: Option<Receiver<LoaderMessage>>,
}

impl Default for WellDataApp {
    fn default() -> Self {
        Self {
            raw_data: Vec::new(),
            available_years: Vec::new(),
            unique_wells: Vec::new(),
            source_file_path: None,
            selected_start_year: None,
            selected_wells: HashSet::new(),
            search_query: String::new(),
            status_message: "Файл не выбран".to_string(),
            is_loading: false,
            progress_global: 0.0,
            progress_local: 0.0,
            rx: None,
        }
    }
}

impl WellDataApp {
    fn new(_cc: &eframe::CreationContext<'_>) -> Self {
        Self::default()
    }

    fn load_file(&mut self) {
        if let Some(path) = FileDialog::new().add_filter("Excel", &["xlsx"]).pick_file() {
            self.source_file_path = Some(path.to_string_lossy().to_string());
            self.start_worker(move |tx| read_excel_file(&path, tx));
        }
    }

    fn process_data(&mut self) {
        if self.raw_data.is_empty() {
            return;
        }

        let start_year = match self.selected_start_year {
            Some(y) => y,
            None => {
                self.status_message = "Выберите год!".to_string();
                return;
            }
        };
        if self.selected_wells.is_empty() {
            self.status_message = "Выберите скважины!".to_string();
            return;
        }

        if let Some(path) = FileDialog::new().add_filter("Excel", &["xlsx"]).save_file() {
            let data = self.raw_data.clone();
            let wells = self.selected_wells.clone();

            self.start_worker(move |tx| save_excel_file(&path, &data, start_year, &wells, tx));
        }
    }

    fn start_worker<F>(&mut self, task: F)
    where
        F: FnOnce(Sender<LoaderMessage>) -> Result<LoaderMessage, Box<dyn Error + Send + Sync>>
            + Send
            + 'static,
    {
        self.is_loading = true;
        self.progress_global = 0.0;
        self.progress_local = 0.0;
        self.status_message = "Запуск...".to_string();

        let (tx, rx) = channel();
        self.rx = Some(rx);
        let tx_for_thread = tx.clone();

        thread::spawn(move || match task(tx_for_thread.clone()) {
            Ok(msg) => {
                let _ = tx_for_thread.send(msg);
            }
            Err(e) => {
                let _ = tx_for_thread.send(LoaderMessage::Error(e.to_string()));
            }
        });
    }
}

// --- ФУНКЦИИ РАБОТЫ С ДАННЫМИ ---

fn read_excel_file(
    path: &PathBuf,
    tx: Sender<LoaderMessage>,
) -> Result<LoaderMessage, Box<dyn Error + Send + Sync>> {
    let _ = tx.send(LoaderMessage::Progress(
        0.0,
        0.0,
        "Открытие файла...".to_string(),
    ));

    let mut workbook: Xlsx<_> = calamine::open_workbook(path)?;
    let sheets = workbook.sheet_names().to_owned();
    let total_sheets = sheets.len();

    let mut all_records = Vec::new();
    let mut valid_years = BTreeSet::new();
    let mut unique_wells = BTreeSet::new();

    for (sheet_idx, sheet_name) in sheets.iter().enumerate() {
        let global_prog = sheet_idx as f32 / total_sheets as f32;

        let _ = tx.send(LoaderMessage::Progress(
            global_prog,
            0.0,
            format!("Лист '{}': чтение и парсинг (ждите)...", sheet_name),
        ));

        if let Ok(year) = sheet_name.parse::<i32>() {
            if let Ok(range) = workbook.worksheet_range(sheet_name) {
                let total_rows_in_sheet = range.height();

                let mut headers = range.rows().next().ok_or("Пустой лист")?.iter();
                let mut col_map = std::collections::HashMap::new();
                for (i, cell) in headers.enumerate() {
                    if let Some(s) = cell.get_string() {
                        col_map.insert(s.to_string(), i);
                    }
                }

                if let (Some(&idx_n), Some(&idx_d)) = (col_map.get(NAME_COL), col_map.get("Date")) {
                    valid_years.insert(year);
                    let idx_liq = col_map.get("PdLiq").copied();
                    let idx_oil = col_map.get("PdOil").copied();
                    let idx_temp = col_map.get(TEMPERATURE_COL).copied();

                    for (i, row) in range.rows().skip(1).enumerate() {
                        if i % 5000 == 0 {
                            let local_prog = i as f32 / total_rows_in_sheet as f32;
                            let _ = tx.send(LoaderMessage::Progress(
                                global_prog,
                                local_prog,
                                format!("Лист '{}': обработка строк...", sheet_name),
                            ));
                        }

                        let well_name = match row.get(idx_n) {
                            Some(Data::String(s)) => s.clone(),
                            Some(Data::Float(f)) => f.to_string(),
                            Some(Data::Int(i)) => i.to_string(),
                            _ => continue,
                        };

                        let date = match row.get(idx_d) {
                            Some(d) => d.as_datetime(),
                            None => None,
                        };

                        let get_float = |idx_opt: Option<usize>| -> Option<f64> {
                            idx_opt.and_then(|i| row.get(i).and_then(|c| c.get_float()))
                        };

                        unique_wells.insert(well_name.clone());
                        all_records.push(WellRecord {
                            well_name,
                            date,
                            pd_liq: get_float(idx_liq),
                            pd_oil: get_float(idx_oil),
                            temperature: get_float(idx_temp),
                            year_sheet: year,
                        });
                    }
                }
            }
        }
    }

    let _ = tx.send(LoaderMessage::Progress(
        1.0,
        1.0,
        "Финализация...".to_string(),
    ));
    Ok(LoaderMessage::Loaded((
        all_records,
        valid_years.into_iter().collect(),
        unique_wells.into_iter().collect(),
    )))
}

fn save_excel_file(
    path: &PathBuf,
    data: &[WellRecord],
    start_year: i32,
    selected_wells: &HashSet<String>,
    tx: Sender<LoaderMessage>,
) -> Result<LoaderMessage, Box<dyn Error + Send + Sync>> {
    let _ = tx.send(LoaderMessage::Progress(
        0.0,
        0.0,
        "Подготовка данных...".to_string(),
    ));

    let mut filtered_data: Vec<&WellRecord> = data
        .iter()
        .filter(|r| r.year_sheet >= start_year && selected_wells.contains(&r.well_name))
        .collect();

    filtered_data.sort_by(|a, b| a.well_name.cmp(&b.well_name).then(a.date.cmp(&b.date)));

    let mut workbook = Workbook::new();
    let wells_to_export: Vec<&String> = filtered_data
        .iter()
        .map(|r| &r.well_name)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    let total_wells = wells_to_export.len();

    for (idx, well_name) in wells_to_export.iter().enumerate() {
        let global_prog = idx as f32 / total_wells as f32;
        let _ = tx.send(LoaderMessage::Progress(
            global_prog,
            0.0,
            format!("Запись скважины: {}", well_name),
        ));

        let safe_name = well_name.replace(['/', '\\', '?', '*', '[', ']'], "_");
        let sheet_name = if safe_name.len() > 30 {
            &safe_name[..30]
        } else {
            &safe_name
        };

        let worksheet = workbook.add_worksheet().set_name(sheet_name)?;

        worksheet.write_string(0, 0, NAME_COL)?;
        worksheet.write_string(0, 1, "Date")?;
        worksheet.write_string(0, 2, "PdLiq")?;
        worksheet.write_string(0, 3, "PdOil")?;
        worksheet.write_string(0, 4, TEMPERATURE_COL)?;

        let records_for_well: Vec<&&WellRecord> = filtered_data
            .iter()
            .filter(|r| &r.well_name == *well_name)
            .collect();

        let total_rows = records_for_well.len();

        let mut row_idx = 1;
        for (i, record) in records_for_well.iter().enumerate() {
            if i % 500 == 0 {
                let local_prog = i as f32 / total_rows as f32;
                let _ = tx.send(LoaderMessage::Progress(
                    global_prog,
                    local_prog,
                    format!("Скважина {}: строка {}/{}", well_name, i, total_rows),
                ));
            }

            worksheet.write_string(row_idx, 0, &record.well_name)?;
            if let Some(d) = record.date {
                worksheet.write_string(row_idx, 1, d.format("%Y-%m-%d %H:%M:%S").to_string())?;
            }
            if let Some(v) = record.pd_liq {
                worksheet.write_number(row_idx, 2, v)?;
            }
            if let Some(v) = record.pd_oil {
                worksheet.write_number(row_idx, 3, v)?;
            }
            if let Some(v) = record.temperature {
                worksheet.write_number(row_idx, 4, v)?;
            }
            row_idx += 1;
        }
    }

    let _ = tx.send(LoaderMessage::Progress(
        1.0,
        1.0,
        "Сохранение файла на диск...".to_string(),
    ));
    workbook.save(path)?;
    Ok(LoaderMessage::Saved(path.to_string_lossy().to_string()))
}

// --- ИНТЕРФЕЙС ---

impl eframe::App for WellDataApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        let mut should_close_channel = false;

        if let Some(rx) = &self.rx {
            while let Ok(msg) = rx.try_recv() {
                match msg {
                    LoaderMessage::Progress(global, local, text) => {
                        self.progress_global = global;
                        self.progress_local = local;
                        self.status_message = text;
                    }
                    LoaderMessage::Loaded((data, years, wells)) => {
                        self.raw_data = data;
                        self.available_years = years;
                        self.unique_wells = wells;
                        if let Some(first) = self.available_years.first() {
                            self.selected_start_year = Some(*first);
                        }
                        self.status_message =
                            format!("Готово. Загружено: {} записей", self.raw_data.len());
                        self.is_loading = false;
                        should_close_channel = true;
                    }
                    LoaderMessage::Saved(path) => {
                        self.status_message = format!("Успех! Файл сохранен: {}", path);
                        self.is_loading = false;
                        should_close_channel = true;
                    }
                    LoaderMessage::Error(e) => {
                        self.status_message = format!("ОШИБКА: {}", e);
                        self.is_loading = false;
                        should_close_channel = true;
                    }
                }
            }
        }

        if should_close_channel {
            self.rx = None;
        }

        if self.is_loading {
            ctx.request_repaint();
        }

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("Обработка данных скважин");
            ui.add_space(5.0);

            ui.set_enabled(!self.is_loading);

            // 1. Файл
            ui.horizontal(|ui| {
                if ui.button("📂 Открыть файл").clicked() {
                    self.load_file();
                }
                ui.label(self.source_file_path.as_deref().unwrap_or("..."));
            });

            // 2. Год
            ui.horizontal(|ui| {
                ui.label("📅 Год начала:");
                let txt = self
                    .selected_start_year
                    .map(|y| y.to_string())
                    .unwrap_or_default();
                egui::ComboBox::from_id_source("y")
                    .selected_text(txt)
                    .show_ui(ui, |ui| {
                        for y in &self.available_years {
                            ui.selectable_value(
                                &mut self.selected_start_year,
                                Some(*y),
                                y.to_string(),
                            );
                        }
                    });
            });

            ui.separator();

            // =========================================================
            //               ДВУХКОЛОНОЧНЫЙ ИНТЕРФЕЙС
            // =========================================================

            // Задаем 2 колонки
            ui.columns(2, |columns| {
                // --- ЛЕВАЯ КОЛОНКА: ПОИСК И ВЫБОР ---
                columns[0].vertical(|ui| {
                    ui.heading("🔍 Поиск");

                    // Строка поиска
                    ui.horizontal(|ui| {
                        ui.text_edit_singleline(&mut self.search_query);
                        if !self.search_query.is_empty() && ui.button("✖").clicked() {
                            self.search_query.clear();
                        }
                    });

                    // Фильтрация
                    let filtered_wells: Vec<&String> = self
                        .unique_wells
                        .iter()
                        .filter(|w| w.to_lowercase().contains(&self.search_query.to_lowercase()))
                        .collect();

                    if ui.button("Выбрать видимые").clicked() {
                        for well in &filtered_wells {
                            self.selected_wells.insert((*well).clone());
                        }
                    }

                    ui.add_space(5.0);

                    // Список (левый)
                    ui.push_id("left_list", |ui| {
                        egui::ScrollArea::vertical()
                            .max_height(300.0)
                            .show(ui, |ui| {
                                if filtered_wells.is_empty() && !self.unique_wells.is_empty() {
                                    ui.label("Нет совпадений");
                                }
                                for well in filtered_wells {
                                    let mut is_sel = self.selected_wells.contains(well);
                                    if ui.checkbox(&mut is_sel, well).changed() {
                                        if is_sel {
                                            self.selected_wells.insert(well.clone());
                                        } else {
                                            self.selected_wells.remove(well);
                                        }
                                    }
                                }
                            });
                    });
                });

                // --- ПРАВАЯ КОЛОНКА: ВЫБРАННЫЕ ---
                columns[1].vertical(|ui| {
                    ui.heading(format!("✅ Выбрано: {}", self.selected_wells.len()));

                    if ui.button("🗑 Сбросить всё").clicked() {
                        self.selected_wells.clear();
                    }

                    ui.add_space(5.0);

                    // Сортируем выбранные, чтобы список не прыгал
                    let mut sorted_selected: Vec<String> =
                        self.selected_wells.iter().cloned().collect();
                    sorted_selected.sort();

                    // Список (правый)
                    ui.push_id("right_list", |ui| {
                        egui::ScrollArea::vertical()
                            .max_height(300.0)
                            .show(ui, |ui| {
                                if sorted_selected.is_empty() {
                                    ui.label(
                                        egui::RichText::new("Список пуст")
                                            .color(egui::Color32::GRAY),
                                    );
                                }

                                // Отображаем список выбранных с кнопкой удаления
                                for well in sorted_selected {
                                    ui.horizontal(|ui| {
                                        if ui.button("✖").clicked() {
                                            self.selected_wells.remove(&well);
                                        }
                                        ui.label(&well);
                                    });
                                }
                            });
                    });
                });
            });

            ui.add_space(10.0);
            ui.separator();

            // 4. Кнопка
            let ready = !self.raw_data.is_empty()
                && self.selected_start_year.is_some()
                && !self.selected_wells.is_empty();
            if ui
                .add_enabled(
                    ready,
                    egui::Button::new("🚀 Сформировать отчет").min_size(egui::vec2(0.0, 30.0)),
                )
                .clicked()
            {
                self.process_data();
            }

            ui.add_space(10.0);

            // --- БЛОК ПРОГРЕССА ---
            ui.set_enabled(true);

            if self.is_loading {
                ui.label(egui::RichText::new(&self.status_message).strong());
                ui.add_space(5.0);
                ui.label("Общий прогресс:");
                ui.add(egui::ProgressBar::new(self.progress_global).animate(true));

                ui.add_space(5.0);
                if self.progress_local < 0.01 {
                    ui.horizontal(|ui| {
                        ui.spinner();
                        ui.label("Обработка данных...");
                    });
                } else {
                    ui.add(egui::ProgressBar::new(self.progress_local).animate(true));
                }
            } else {
                ui.label(egui::RichText::new(&self.status_message).color(egui::Color32::GRAY));
            }
        });
    }
}

fn main() -> eframe::Result<()> {
    eframe::run_native(
        "Well Data App",
        eframe::NativeOptions {
            // Увеличили ширину, чтобы влезли 2 колонки
            viewport: egui::ViewportBuilder::default().with_inner_size([700.0, 650.0]),
            ..Default::default()
        },
        Box::new(|cc| Ok(Box::new(WellDataApp::new(cc)))),
    )
}
