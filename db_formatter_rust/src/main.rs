use chrono::{DateTime, Datelike};
use clickhouse::{Client, Row};
use core::str;
use rayon::prelude::*;
use serde_json::{de, value};
use std::collections::btree_map::Range;
use std::io::{BufReader, Write};
use std::path::Path;
use std::{clone, fs, io};
use std::{error::Error, fs::File, print, println};

#[derive(Debug, Default, clickhouse::Row, serde::Serialize, PartialEq, Eq, PartialOrd, Ord)]
// #[derive(Debug, Default)]
pub struct Person {
    pub phone: u64,
    pub email: String,
    pub username: String,

    pub surname: String,
    pub name: String,
    pub patronymic: String,
    pub birth_day: u8,
    pub birth_month: u8,
    pub birth_year: u16,

    pub passport: String,
    pub inn: String,
    pub snils: String,
    pub car_number: String,

    pub telegram_id: i64,
    pub source_id: String,

    pub extra: String, // Сюда закинем JSON со всем остальным
}

#[derive(Debug, Clone, Default)]
pub struct ColumnMapping {
    pub fio: Option<usize>,
    pub name: Option<usize>,
    pub surname: Option<usize>,
    pub patronymic: Option<usize>,
    pub phone: Option<usize>,
    pub email: Option<usize>,
    pub username: Option<usize>,
    pub passport: Option<usize>,
    pub inn: Option<usize>,
    pub snils: Option<usize>,
    pub birth_date: Option<usize>,
    pub car_number: Option<usize>,
    pub telegram_id: Option<usize>,
}

#[derive(Debug, clickhouse::Row, serde::Serialize, Default)]
pub struct Meta {
    pub source_id: String,
    pub name: String,
    pub description: String,
    pub leak_date: u16,
    pub schema_mapping: String,
}

fn create_client() -> Client {
    Client::default()
        .with_url("http://127.0.0.1:8123")
        .with_user("admin")
        .with_password("qwerty123")
        .with_database("aura")
}

fn phone(phone_number: &str) -> String {
    // 1. Оставляем только цифры
    let digits: String = phone_number
        .chars()
        .filter(|c| c.is_ascii_digit())
        .collect();

    // 2. Если строка пустая — отдаем как есть
    if digits.is_empty() {
        return digits;
    }

    // 3. Нормализуем первую цифру (7 или 8 переводим в 8)
    if (digits.starts_with('7') || digits.starts_with('8')) && digits.len() == 11 {
        return format!("7{}", &digits[1..]);
    } else {
        digits
    }
}

fn parse_birth(birth_str: &str) -> (u16, u8, u8) {
    // 1. Убираем пробелы, \r, \n и кавычки по краям
    let clean = birth_str.trim().trim_matches(|c| c == '"' || c == '\'');

    // 2. Если там прилипло время ("01.07.1991 00:00:00"), берем только дату
    let date_part = clean.split_whitespace().next().unwrap_or("");

    let parts: Vec<&str> = date_part
        .split(|c| c == '-' || c == '.' || c == '/')
        .collect();

    match parts.as_slice() {
        // Формат: YYYY-MM-DD -> (Год, Месяц, День)
        [y, m, d] if y.len() == 4 => (
            y.parse::<u16>().unwrap_or(0),
            m.parse::<u8>().unwrap_or(0),
            d.parse::<u8>().unwrap_or(0),
        ),
        // Формат: DD-MM-YYYY -> (Год, Месяц, День)
        [d, m, y] if y.len() == 4 => (
            y.parse::<u16>().unwrap_or(0),
            m.parse::<u8>().unwrap_or(0),
            d.parse::<u8>().unwrap_or(0),
        ),
        _ => (0, 0, 0),
    }
}

fn parse_unix_birthday(ts_str: &str) -> (u16, u8, u8) {
    ts_str
        .trim()
        .parse::<i64>()
        .ok()
        .and_then(|ts| DateTime::from_timestamp(ts + 3 * 3600, 0)) // +3 часа под MSK
        .map(|dt| (dt.year() as u16, dt.month() as u8, dt.day() as u8))
        .unwrap_or((0, 0, 0))
}

fn date_to_days(date_str: &str) -> u16 {
    let parts: Vec<u32> = date_str
        .split('-')
        .filter_map(|s| s.parse::<u32>().ok())
        .collect();

    if let [year, month, day] = parts.as_slice() {
        // Простой алгоритм вычисления дней от эпохи Unix
        let (y, m) = if *month <= 2 {
            (year - 1, month + 9)
        } else {
            (*year, month - 3)
        };
        let c = y / 100;
        let ya = y - 100 * c;
        let days_from_civil = (146097 * c) / 4 + (1461 * ya) / 4 + (153 * m + 2) / 5 + day - 1;
        // 719468 — смещение дней от 0000-03-01 до 1970-01-01
        if days_from_civil >= 719468 {
            (days_from_civil - 719468) as u16
        } else {
            0
        }
    } else {
        0
    }
}

fn parse_row_to_person(
    row: &csv::ByteRecord,
    column_mapping: &ColumnMapping,
    unused_idx: &Vec<usize>,
    source_id: &String,
) -> Person {
    let raw_birth = if column_mapping.birth_date != None {
        row.get(column_mapping.birth_date.unwrap())
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .unwrap_or_default()
    } else {
        "".to_string()
    };

    let (year, month, day) = if raw_birth.trim().is_empty() {
        (0, 0, 0)
    } else if !(raw_birth.contains('.') || raw_birth.contains('-') || raw_birth.contains('/')) {
        parse_unix_birthday(&raw_birth)
    } else {
        parse_birth(&raw_birth)
    };

    let mut extra_collect: Vec<String> = Vec::with_capacity(unused_idx.len());
    for &idx in unused_idx {
        let val = row
            .get(idx)
            .and_then(|b| std::str::from_utf8(b).ok())
            .map(|s| s.trim())
            .unwrap_or("");
        extra_collect.push(val.to_string());
    }
    let extra_json = serde_json::to_string(&extra_collect).ok();
    let mut surname = String::new();
    let mut name = String::new();
    let mut patronymic = String::new();

    if let Some(fio_idx) = column_mapping.fio {
        if let Some(bytes) = row.get(fio_idx) {
            let raw_fio = String::from_utf8_lossy(bytes);
            let mut parts = raw_fio.split_whitespace();

            // 1-е слово: Фамилия
            if let Some(s) = parts.next() {
                surname = s.trim().to_string();
            }
            // 2-е слово: Имя
            if let Some(n) = parts.next() {
                name = n.trim().to_string();
            }
            // 3-е слово (и всё, что дальше, если двойное): Отчество
            let rest: Vec<&str> = parts.collect();
            if !rest.is_empty() {
                patronymic = rest.join(" ");
            }
        }
    }
    let current_person = Person {
        source_id: source_id.clone(),
        phone: if column_mapping.phone != None {
            row.get(column_mapping.phone.unwrap())
                .and_then(|b| std::str::from_utf8(b).ok())
                .map(|s| s.trim())
                .map(|s| phone(s))
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0)
        } else {
            0
        },
        email: if column_mapping.email != None {
            row.get(column_mapping.email.unwrap())
                .and_then(|b| std::str::from_utf8(b).ok())
                .map(|s| s.trim().to_lowercase())
                .unwrap_or_default()
        } else {
            String::new()
        },
        username: if column_mapping.username != None {
            row.get(column_mapping.username.unwrap())
                .and_then(|b| std::str::from_utf8(b).ok())
                .map(|s| s.trim().to_lowercase())
                .unwrap_or_default()
        } else {
            String::new()
        },
        surname: if column_mapping.surname != None {
            row.get(column_mapping.surname.unwrap())
                .and_then(|b| std::str::from_utf8(b).ok())
                .map(|s| s.trim().to_lowercase())
                .unwrap_or_default()
        } else if !surname.is_empty() {
            surname
        } else {
            String::new()
        },
        // .map(|s| s.to_string()),
        name: if column_mapping.name != None {
            row.get(column_mapping.name.unwrap())
                .and_then(|b| std::str::from_utf8(b).ok())
                .map(|s| s.trim().to_lowercase())
                .unwrap_or_default()
        } else if !name.is_empty() {
            name
        } else {
            String::new()
        },
        // .map(|s| s.to_string()),
        patronymic: if column_mapping.patronymic != None {
            row.get(column_mapping.patronymic.unwrap())
                .and_then(|b| std::str::from_utf8(b).ok())
                .map(|s| s.trim().to_lowercase())
                .unwrap_or_default()
        } else if !patronymic.is_empty() {
            patronymic
        } else {
            String::new()
        },
        extra: extra_json.unwrap(),
        // birth
        birth_day: day,
        birth_month: month,
        birth_year: year,

        passport: if column_mapping.passport != None {
            row.get(column_mapping.passport.unwrap())
                .and_then(|b| std::str::from_utf8(b).ok())
                .map(|s| s.trim())
                .map(|s| s.to_lowercase())
                .unwrap_or_default()
                .chars()
                .take(10)
                .collect()
        } else {
            String::new()
        },

        inn: if column_mapping.inn != None {
            row.get(column_mapping.inn.unwrap())
                .and_then(|b| std::str::from_utf8(b).ok())
                .map(|s| s.trim())
                .map(|s| s.to_lowercase())
                .unwrap_or_default()
        } else {
            String::new()
        },

        snils: if column_mapping.snils != None {
            row.get(column_mapping.snils.unwrap())
                .and_then(|b| std::str::from_utf8(b).ok())
                .map(|s| s.trim())
                .map(|s| s.to_lowercase())
                .unwrap_or_default()
        } else {
            String::new()
        },

        car_number: if column_mapping.car_number != None {
            row.get(column_mapping.car_number.unwrap())
                .and_then(|b| std::str::from_utf8(b).ok())
                .map(|s| s.trim().to_lowercase())
                .unwrap_or_default()
        } else {
            String::new()
        },

        telegram_id: if column_mapping.telegram_id != None {
            row.get(column_mapping.telegram_id.unwrap())
                .and_then(|b| std::str::from_utf8(b).ok())
                .map(|s| s.trim())
                .unwrap_or_default()
                .parse::<i64>()
                .unwrap_or_default()
        } else {
            0
        },

        ..Default::default()
    };

    current_person
}

async fn db_reader(
    db_name: &str,
    column_mapping: &ColumnMapping,
    unused_idx: &Vec<usize>,
    source_id: &String,
    delimiter: u8,
) -> Result<(), Box<dyn Error>> {
    println!("Начало читки файла!");
    let mut raw_chunk: Vec<csv::ByteRecord> = Vec::with_capacity(100_000);
    let db = File::open(db_name)?;
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(true)
        .flexible(true) // разрешает кривые длины строк
        .quoting(false)
        .from_reader(BufReader::with_capacity(4 * 1024 * 1024, db));

    let mut record = csv::ByteRecord::new();
    let client = create_client();

    while let Ok(true) = rdr.read_byte_record(&mut record) {
        raw_chunk.push(record.clone());
        if raw_chunk.len() == 100_000 {
            println!("Пошла обработка 100.000!");
            let mut processed_chunk: Vec<Person> = raw_chunk
                .par_iter()
                .map(|row| parse_row_to_person(row, column_mapping, unused_idx, source_id))
                .collect();
            processed_chunk.par_sort_unstable();
            println!(
                "Чанк на {} строк успешно обработан в многопотоке!",
                processed_chunk.len()
            );
            let len_before = processed_chunk.len();
            processed_chunk.dedup();
            let len_after = processed_chunk.len();

            println!(
                "Удалено дублей в батче: {} (осталось {})",
                len_before - len_after,
                len_after
            );
            push_to_click_db(&client, processed_chunk).await;
            println!("Чанк загружен в DB!\n");

            raw_chunk.clear();
        }
    }
    if !raw_chunk.is_empty() {
        println!("Пошла обработка Остатка!");
        let mut processed_chunk: Vec<Person> = raw_chunk
            .par_iter()
            .map(|record| parse_row_to_person(record, column_mapping, unused_idx, source_id))
            .collect();
        println!(
            "Чанк на {} строк успешно обработан в многопотоке!",
            processed_chunk.len()
        );
        processed_chunk.sort(); // реализуй Ord / Eq для Person
        let len_before = processed_chunk.len();
        processed_chunk.dedup();
        let len_after = processed_chunk.len();

        println!(
            "Удалено дублей в батче: {} (осталось {})",
            len_before - len_after,
            len_after
        );
        push_to_click_db(&client, processed_chunk).await;
        println!("Чанк загружен в DB!\n");
        raw_chunk.clear();
    }

    Ok(())
}

async fn push_to_click_db(client: &clickhouse::Client, push_vec: Vec<Person>) {
    let mut insert = client.insert::<Person>("records").await.unwrap();
    for record in push_vec {
        insert.write(&record).await.unwrap();
    }
    insert.end().await.unwrap();
}

async fn push_sources_info_to_click_db(client: &clickhouse::Client, pushing_info: Meta) {
    let mut insert = client.insert::<Meta>("sources_info").await.unwrap();
    insert.write(&pushing_info).await.unwrap();
    insert.end().await.unwrap();
}

// fn detect_delimiter(header_line: &str) -> u8 {
//     let delimiters = [b'\t', b',', b';', b'|'];

//     delimiters
//         .into_iter()
//         .max_by_key(|&d| header_line.as_bytes().iter().filter(|&&b| b == d).count())
//         .unwrap_or(b',')
// }

fn ask(prompt: &str) -> String {
    print!("{}", prompt);
    io::stdout().flush().ok();

    let mut s = String::new();
    std::io::stdin().read_line(&mut s).ok();
    s.trim().to_string()
}

fn ask_idx(prompt: &str) -> Option<usize> {
    let input = ask(prompt);
    let trimmed = input.trim();
    if trimmed.is_empty() {
        None
    } else {
        trimmed.parse::<usize>().ok()
    }
}

#[tokio::main]
async fn main() {
    let db_name = ask("name:");
    let description = ask("description:");
    let leak_date = date_to_days(&ask("leak_date (YYYY-MM-DD):"));
    let db_path = ask("DB path (trash-dbs/cdek2021/):");
    let mut delimiter = ask("delimiter (default: tab):");
    if delimiter.is_empty() {
        delimiter = "\t".to_string();
    }
    let source_id: String = Path::new(&db_path)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown")
        .to_string();

    println!("Start parsing!");
    let mut is_first: bool = true;
    let mut headers: Vec<String> = Vec::new();
    let mut column_mapping: ColumnMapping = ColumnMapping::default();
    let mut meta_info: Meta = Meta::default();
    let mut used_idx: Vec<usize> = Vec::new();
    // [id, fio....] from csv
    let mut unused_idx: Vec<usize> = Vec::new();
    let mut unused_headers: Vec<String> = Vec::new();

    for part in fs::read_dir(&db_path).unwrap() {
        let path_db = part.unwrap().path();
        if is_first {
            let rdr = csv::ReaderBuilder::new()
                .delimiter(delimiter.as_bytes()[0])
                .has_headers(true)
                .from_path(&path_db);
            headers = rdr
                .unwrap()
                .byte_headers()
                .unwrap()
                .iter()
                .map(|bytes| String::from_utf8_lossy(bytes).trim().to_string())
                .collect();

            // creating map for headers, aka name=1,surname=2,emain=7,number=8
            for (i, h) in headers.iter().enumerate() {
                println!("[{}] {}", i, h);
            }
            let fio_type = ask("1) fio; \n2) name, surname, patronymic\n Enter to pass: ");

            let mut fio: Option<usize> = None;

            let mut name: Option<usize> = None;
            let mut surname: Option<usize> = None;
            let mut patronymic: Option<usize> = None;

            if fio_type.trim() == "1" {
                fio = ask_idx("fio: ");
            } else if fio_type.trim() == "2" {
                name = ask_idx("name: ");
                surname = ask_idx("surname: ");
                patronymic = ask_idx("patronymic: ");
            } else {
            }

            let phone = ask_idx("mobile phone: ");
            let email = ask_idx("email: ");
            let username = ask_idx("username: ");
            let passport = ask_idx("passport: ");
            let inn = ask_idx("inn: ");
            let snils = ask_idx("snils: ");
            let birth_date = ask_idx("birth_day: ");
            let car_number = ask_idx("car_number: ");
            let telegram_id = ask_idx("telegram_id: ");

            column_mapping = ColumnMapping {
                fio: fio,
                name: name,
                surname: surname,
                patronymic: patronymic,
                phone: phone,
                email: email,
                username: username,
                passport: passport,
                inn: inn,
                snils: snils,
                birth_date: birth_date,
                car_number: car_number,
                telegram_id: telegram_id,
            };

            let m = column_mapping.clone();
            used_idx = [
                m.fio,
                m.name,
                m.surname,
                m.patronymic,
                m.phone,
                m.email,
                m.username,
                m.passport,
                m.inn,
                m.snils,
                m.birth_date,
                m.car_number,
                m.telegram_id,
            ]
            .into_iter()
            .flatten()
            .collect();

            for (idx, header_name) in headers.iter().enumerate() {
                if !used_idx.contains(&idx) {
                    unused_idx.push(idx);
                    unused_headers.push(header_name.trim().to_lowercase());
                }
            }

            let schema_mapping = serde_json::to_string(&unused_headers).unwrap_or_default();

            meta_info = Meta {
                source_id: source_id.clone(),
                name: db_name.to_string(),
                description: description.to_string(),
                leak_date: leak_date,
                schema_mapping: schema_mapping,
            };
            let client = create_client();
            push_sources_info_to_click_db(&client, meta_info).await;
        }

        is_first = false;
        if db_reader(
            path_db.to_str().unwrap(),
            &column_mapping,
            &unused_idx,
            &source_id,
            delimiter.as_bytes()[0],
        )
        .await
        .is_ok()
        {
            println!("Success");
        } else {
            println!("Error");
        }
    }

    // match db_reader("trash-dbs/test_full.csv") {
    //     Ok(()) => println!("Sucess"),
    //     Err(err) => println!("error running example: {}", err),
    // }
}
