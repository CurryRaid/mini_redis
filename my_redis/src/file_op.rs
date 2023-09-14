use std::{fs::{OpenOptions, File}, io::{BufReader, BufRead, Write}};


pub fn write_to_file(file_path: &str, key: &str, value: &str) -> std::io::Result<()> {
    // 打开文件并追加写入
    let mut file = OpenOptions::new()
        .append(true)
        .open(file_path)?;

    // 写入键值对，并在它们之间添加空格
    file.write_all(key.as_bytes())?;
    file.write_all(b" ")?;
    file.write_all(value.as_bytes())?;

    // 写入换行符
    file.write_all(b"\n")?;

    Ok(())
}

pub fn update_key_value_in_file(file_path: &str, key: &str, new_value: &str) -> std::io::Result<()> {
    // 打开原始文件进行读取
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);

    // 创建临时文件
    let temp_file_path = format!("{}_temp", file_path);
    let mut temp_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temp_file_path)?;

    // 逐行读取原始文件并写入临时文件
    for line in reader.lines() {
        if let Ok(line) = line {
            let tokens: Vec<&str> = line.split_whitespace().collect();
            if tokens.len() >= 2 && tokens[0] == key {
                writeln!(temp_file, "{} {}", key, new_value)?;
            } else {
                writeln!(temp_file, "{}", line)?;
            }
        }
    }
    // 关闭文件
    drop(temp_file);
    // 删除原始文件
    std::fs::remove_file(file_path)?;
    // 将临时文件重命名为原始文件
    std::fs::rename(&temp_file_path, file_path)?;
    Ok(())
}

pub fn remove_key_value_from_file(file_path: &str, key: &str) -> std::io::Result<()> {
    // 打开原始文件进行读取
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);

    // 创建临时文件
    let temp_file_path = format!("{}_temp", file_path);
    let mut temp_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temp_file_path)?;

    // 逐行读取原始文件并写入临时文件，忽略需要删除的键值对
    for line in reader.lines() {
        if let Ok(line) = line {
            let tokens: Vec<&str> = line.split_whitespace().collect();
            if tokens.len() >= 2 && tokens[0] != key {
                writeln!(temp_file, "{}", line)?;
            }
        }
    }
    // 关闭文件
    drop(temp_file);
    // 删除原始文件
    std::fs::remove_file(file_path)?;
    // 将临时文件重命名为原始文件
    std::fs::rename(&temp_file_path, file_path)?;
    Ok(())
}