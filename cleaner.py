#!/usr/bin/env python3

import os
import pymysql
import shutil
import configparser
import subprocess
import getpass
import sys
import re
import json
from datetime import datetime
from shutil import rmtree, copytree, copy2
import ast
import tempfile
import pwd
import grp

def get_uid_gid(user_str):
    """Получает UID и GID пользователя по имени"""
    try:
        pw_record = pwd.getpwnam(user_str)
        return pw_record.pw_uid, pw_record.pw_gid
    except KeyError:
        print(f"[ERROR] Пользователь '{user_str}' не найден в системе.")
        return None, None

def apply_permissions(path, uid, gid):
    """Рекурсивно назначает права пользователю на путь"""
    try:
        os.chown(path, uid, gid)
        for root, dirs, files in os.walk(path):
            for d in dirs:
                os.chown(os.path.join(root, d), uid, gid)
            for f in files:
                os.chown(os.path.join(root, f), uid, gid)
    except PermissionError:
        print(f"[WARNING] Не удалось сменить владельца {path}. Попробуйте запустить скрипт с sudo.")
    except Exception as e:
        print(f"[ERROR] Ошибка назначения прав на {path}: {e}")

def unquote_value(value):
    """Удаляет кавычки из значения, если они есть"""
    if value is None:
        return value
    value_str = str(value).strip()
    if (value_str.startswith('"') and value_str.endswith('"')) or \
            (value_str.startswith("'") and value_str.endswith("'")):
        return value_str[1:-1]
    return value_str


def parse_quoted_list(value):
    """Парсит список значений, которые могут быть в кавычках"""
    if not value:
        return []

    pattern = r'\"[^\"]+\"|\'[^\']+\'|[^,\s]+'
    matches = re.findall(pattern, value)

    result = []
    for match in matches:
        cleaned_value = unquote_value(match)
        if cleaned_value:
            result.append(cleaned_value)
    return result


# --- ИЗМЕНЕННАЯ ФУНКЦИЯ: теперь принимает путь к корню сайта ---
def parse_bitrix_settings(site_root_path):
    """Выполняет .settings.php через PHP-интерпретатор для получения чистых данных"""

    php_file_path = os.path.join(site_root_path, 'bitrix', '.settings.php')

    if not os.path.exists(php_file_path):
        print(f"[DEBUG] Файл {php_file_path} не найден.")
        return None

    try:
        # Проверяем наличие системного php
        subprocess.run(['php', '-v'], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("[ERROR] На сервере не найден исполняемый файл 'php'. Невозможно прочитать настройки Bitrix.")
        return None

    php_code = f"""
<?php
$config = include('{php_file_path}');
$connections = $config['connections']['value']['default'] ?? null;
if ($connections) {{
    echo json_encode([
        'host' => $connections['host'],
        'database' => $connections['database'],
        'user' => $connections['login'],
        'password' => $connections['password']
    ]);
}} else {{
    echo "null";
}}
?>
"""

    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.php', delete=False, encoding='utf-8') as tmp:
            tmp.write(php_code)
            tmp_path = tmp.name

        result = subprocess.run(
            ['php', tmp_path],
            capture_output=True,
            text=True,
            timeout=5,
            cwd=site_root_path  # Важно: запускаем из папки сайта
        )

        os.unlink(tmp_path)

        if result.returncode != 0:
            print(f"[ERROR] Ошибка выполнения PHP-кода:\n{result.stderr.strip()}")
            return None

        output = result.stdout.strip()
        if output == "null" or not output:
            return None

        import json
        data = json.loads(output)

        if all(data.values()):
            return data
        return None

    except Exception as e:
        print(f"[ERROR] Критическая ошибка при вызове PHP: {str(e)}")
        return None


def load_settings():
    """Загрузка настроек из settings.ini или bitrix/.settings.php"""

    config = configparser.ConfigParser(interpolation=None)

    if not os.path.exists('settings.ini'):
        raise FileNotFoundError("Файл настроек settings.ini не найден")

    # Читаем файл сразу, чтобы получить список папок 'clean' до парсинга БД
    config.read('settings.ini', encoding='utf-8')

    settings = {
        'database': {'mode': '', 'tables': [], 'auth_plugin': None},
        'folders': {
            'clean': [], 'copy_sources': [], 'copy_destinations': [],
            'copy_user': '', 'preserve_dirs': [], 'preserve_files': []
        },
        'backup': {'enable': True, 'backup_dir': ''},
        'security': {'confirm_destructive_operations': True}
    }

    # --- ОПРЕДЕЛЕНИЕ КОРНЯ САЙТА ---
    raw_clean_paths = config.get('folders', 'clean', fallback='')
    site_root = ""

    if raw_clean_paths.strip():
        first_clean_path = parse_quoted_list(raw_clean_paths)[0]

        # Делаем путь абсолютным относительно места запуска скрипта
        abs_first_path = os.path.abspath(first_clean_path)

        # Поднимаемся вверх по дереву каталогов, пока не найдем папку 'bitrix'
        current_check = abs_first_path
        while current_check != '/':
            potential_bitrix = os.path.join(current_check, 'bitrix')
            if os.path.isdir(potential_bitrix):
                site_root = current_check
                break

            parent = os.path.dirname(current_check)
            if parent == current_check:  # Достигли корня файловой системы
                break
            current_check = parent

        # Если битрикс так и не нашли (например, чистим /tmp), берем текущую директорию
        if not site_root:
            site_root = os.getcwd()
    else:
        site_root = os.getcwd()

    print(f"[INFO] Корень сайта определен как: {site_root}")

    # --- ПАРСИНГ НАСТРОЕК БАЗЫ ДАННЫХ ---
    bx_db_config = parse_bitrix_settings(site_root)

    use_ini_fallback = False
    if not bx_db_config:
        print("[WARNING] Не удалось извлечь данные из bitrix/.settings.php. Используем settings.ini...")
        use_ini_fallback = True
    else:
        print("[SUCCESS] Настройки БД успешно загружены из bitrix/.settings.php")

    if use_ini_fallback:
        settings['database'].update({
            'host': unquote_value(config.get('database', 'host', fallback='localhost')),
            'user': unquote_value(config.get('database', 'user', fallback='root')),
            'password': unquote_value(config.get('database', 'password', fallback='')),
            'database_name': unquote_value(config.get('database', 'database_name', fallback=''))
        })
    else:
        settings['database'].update({
            'host': bx_db_config['host'],
            'user': bx_db_config['user'],
            'password': bx_db_config['password'],
            'database_name': bx_db_config['database']
        })

    # --- ЧТЕНИЕ ОСТАЛЬНЫХ ПАРАМЕТРОВ ИЗ SETTINGS.INI ---

    # ВАЖНО: Здесь убрано жесткое присваивание mode = 'truncate'.
    # Теперь режим берется строго из файла.
    raw_mode = config.get('database', 'mode', fallback='truncate').strip().lower()
    settings['database']['mode'] = raw_mode

    tables_raw = config.get('database', 'tables', fallback='')
    if tables_raw.strip():
        settings['database']['tables'] = parse_quoted_list(tables_raw)

    auth_plugin_raw = config.get('database', 'auth_plugin', fallback='')
    if auth_plugin_raw.strip():
        settings['database']['auth_plugin'] = unquote_value(auth_plugin_raw)

    clean_raw = config.get('folders', 'clean', fallback='')
    if clean_raw.strip():
        settings['folders']['clean'] = [os.path.normpath(p) for p in parse_quoted_list(clean_raw)]

    copy_src_raw = config.get('folders', 'copy_sources', fallback='')
    if copy_src_raw.strip():
        settings['folders']['copy_sources'] = [os.path.normpath(p) for p in parse_quoted_list(copy_src_raw)]

    copy_dst_raw = config.get('folders', 'copy_destinations', fallback='')
    if copy_dst_raw.strip():
        settings['folders']['copy_destinations'] = [os.path.normpath(p) for p in parse_quoted_list(copy_dst_raw)]

    settings['folders']['copy_user'] = unquote_value(config.get('folders', 'copy_user', fallback=''))

    preserve_dirs_raw = config.get('folders', 'preserve_dirs', fallback='')
    if preserve_dirs_raw.strip():
        settings['folders']['preserve_dirs'] = [os.path.normpath(p) for p in parse_quoted_list(preserve_dirs_raw)]

    preserve_files_raw = config.get('folders', 'preserve_files', fallback='')
    if preserve_files_raw.strip():
        settings['folders']['preserve_files'] = [os.path.normpath(p) for p in parse_quoted_list(preserve_files_raw)]

    settings['backup']['enable'] = config.getboolean('backup', 'enable', fallback=True)
    settings['backup']['backup_dir'] = unquote_value(config.get('backup', 'backup_dir', fallback=''))

    settings['security']['confirm_destructive_operations'] = config.getboolean(
        'security', 'confirm_destructive_operations', fallback=True)

    return settings


def confirm_destructive_operation(operation_description):
    """Запрос подтверждения для деструктивных операций"""
    print(f"\n⚠️  ВНИМАНИЕ: {operation_description}")
    response = input("Продолжить? (y/N): ").strip().lower()
    return response in ['y', 'yes', 'д', 'да']


def run_as_user(command, username):
    """Выполнить команду от имени указанного пользователя"""
    try:
        if username and username != getpass.getuser():
            command = ['sudo', '-u', username] + command
            print(f"Выполнение от пользователя {username}: {' '.join(command)}")

        result = subprocess.run(command, capture_output=True, text=True, check=True)
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        return False, f"Ошибка: {e.stderr}"


def create_db_connection(db_settings):
    """Создание подключения к базе данных с обработкой ошибок"""
    try:
        connection_params = {
            'host': db_settings['host'],
            'user': db_settings['user'],
            'password': db_settings['password'],
            'database': db_settings['database_name'],
            'charset': 'utf8mb4'
        }

        # Добавляем auth_plugin если указан
        if db_settings['auth_plugin']:
            connection_params['auth_plugin'] = db_settings['auth_plugin']

        connection = pymysql.connect(**connection_params)
        return connection, None

    except pymysql.err.OperationalError as e:
        if "cryptography" in str(e):
            error_msg = (
                f"Ошибка подключения к БД: {e}\n"
                "РЕШЕНИЕ: Установите пакет cryptography:\n"
                "pip3 install cryptography\n\n"
                "ИЛИ измените метод аутентификации пользователя MySQL:\n"
                "ALTER USER 'username'@'localhost' IDENTIFIED WITH mysql_native_password BY 'password';\n"
                "FLUSH PRIVILEGES;"
            )
        else:
            error_msg = f"Ошибка подключения к БД: {e}"

        return None, error_msg
    except Exception as e:
        return None, f"Ошибка подключения к БД: {e}"


def get_all_tables(connection):
    """Получить список всех таблиц в базе данных"""
    with connection.cursor() as cursor:
        cursor.execute("SHOW TABLES;")
        tables = [row[0] for row in cursor.fetchall()]
    return tables


def drop_all_tables(connection):
    """Удалить ВСЕ таблицы в базе данных"""
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
            tables = get_all_tables(connection)

            if not tables:
                print("В базе данных нет таблиц для удаления")
                return

            print(f"Найдено таблиц для удаления: {len(tables)}")
            drop_query = "DROP TABLE " + ", ".join(tables) + ";"
            cursor.execute(drop_query)
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
            print(f"Все таблицы ({len(tables)}) успешно удалены")

    except Exception as e:
        print(f"Ошибка при удалении таблиц: {str(e)}")
        try:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        except:
            pass


def drop_specific_tables(connection, tables_list):
    """Удалить указанные таблицы"""
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
            for table in tables_list:
                cursor.execute(f"DROP TABLE IF EXISTS {table};")
                print(f"Таблица {table} удалена")
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
            print(f"Удалено таблиц: {len(tables_list)}")

    except Exception as e:
        print(f"Ошибка при удалении таблиц: {str(e)}")
        try:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        except:
            pass


def truncate_tables(connection, tables_list):
    """Очистить указанные таблицы"""
    try:
        with connection.cursor() as cursor:
            for table in tables_list:
                cursor.execute(f"TRUNCATE TABLE {table};")
                print(f"Таблица {table} очищена")
        connection.commit()
        print("Указанные таблицы успешно очищены")

    except Exception as e:
        print(f"Ошибка при очистке таблиц: {str(e)}")


def clean_database(settings):
    """Основная функция для работы с базой данных"""
    db_settings = settings['database']
    connection, error = create_db_connection(db_settings)
    if error:
        print(error)
        return

    try:
        mode = db_settings['mode']
        tables_list = db_settings['tables']

        if mode == 'drop_all':
            if settings['security']['confirm_destructive_operations']:
                if not confirm_destructive_operation("Будут удалены ВСЕ таблицы в базе данных!"):
                    print("Операция отменена пользователем")
                    return
            print("Режим: УДАЛЕНИЕ ВСЕХ ТАБЛИЦ")
            drop_all_tables(connection)

        elif mode == 'drop_list':
            if settings['security']['confirm_destructive_operations'] and tables_list:
                if not confirm_destructive_operation(f"Будут удалены таблицы: {', '.join(tables_list)}"):
                    print("Операция отменена пользователем")
                    return
            print("Режим: УДАЛЕНИЕ УКАЗАННЫХ ТАБЛИЦ")
            drop_specific_tables(connection, tables_list)

        elif mode == 'truncate':
            print("Режим: ОЧИСТКА УКАЗАННЫХ ТАБЛИЦ")
            truncate_tables(connection, tables_list)

        else:
            print(f"Неизвестный режим работы с БД: {mode}")

    except Exception as e:
        print(f"Ошибка при работе с БД: {str(e)}")
    finally:
        if connection:
            connection.close()


def clean_folders(folders_to_clean, settings):
    """Очистка содержимого папок с сохранением исключений и назначением прав"""
    if not folders_to_clean:
        print("Нет папок для очистки")
        return

    preserve_dirs = [os.path.normpath(p) for p in settings['folders'].get('preserve_dirs', [])]
    preserve_files = [os.path.normpath(p) for p in settings['folders'].get('preserve_files', [])]

    # Определяем корень сайта относительно первой очищаемой папки
    raw_clean_paths = settings['folders']['clean']
    site_root = os.path.abspath(raw_clean_paths[0]) if raw_clean_paths else os.getcwd()
    while 'bitrix' not in os.listdir(site_root) and site_root != '/':
        site_root = os.path.dirname(site_root)

    tmp_dir = os.path.join(os.getcwd(), 'tmp')

    copy_user = settings['folders'].get('copy_user')
    uid, gid = None, None

    if copy_user:
        uid, gid = get_uid_gid(copy_user)
        if not uid:
            print("[ERROR] Работа прекращена из-за ошибки определения пользователя.")
            return

    folder_descriptions = []
    for folder in folders_to_clean:
        rel_dirs = ", ".join([f"'{d}'" for d in preserve_dirs]) if preserve_dirs else "нет"
        rel_files = ", ".join([f"'{f}'" for f in preserve_files]) if preserve_files else "нет"
        folder_descriptions.append(f"{folder} (сохранить папки: {rel_dirs}, файлы: {rel_files})")

    if settings['security']['confirm_destructive_operations']:
        if not confirm_destructive_operation(
                f"ВНИМАНИЕ: Будет выполнена очистка СОДЕРЖИМОГО папок.\nЦелевые папки:\n" + "\n".join(
                    folder_descriptions)):
            print("Операция отменена пользователем")
            return

    try:
        # Шаг 1: Сохранение исключений во временную папку
        preserved_tmp_base = os.path.join(tmp_dir, 'preserved_exceptions')
        if os.path.exists(tmp_dir):
            rmtree(tmp_dir)
        os.makedirs(preserved_tmp_base, exist_ok=True)

        print("\n=== ЭТАП 1: Резервное копирование исключений ===")
        for root_folder in folders_to_clean:
            norm_root = os.path.normpath(root_folder)
            if not os.path.exists(norm_root):
                continue

            for current_dir, subdirs, files in os.walk(norm_root):
                rel_path = os.path.relpath(current_dir, norm_root)

                save_current_dir = False
                for preserve_rel in preserve_dirs:
                    full_preserve_abs = os.path.normpath(os.path.join(site_root, preserve_rel))
                    if current_dir == full_preserve_abs or current_dir.startswith(full_preserve_abs + os.sep):
                        save_current_dir = True
                        break

                if save_current_dir:
                    target_tmp = os.path.join(preserved_tmp_base, rel_path)
                    os.makedirs(target_tmp, exist_ok=True)
                    copytree(current_dir, target_tmp, dirs_exist_ok=True)

                for filename in files:
                    file_abs_path = os.path.join(current_dir, filename)
                    should_save = any(file_abs_path == os.path.normpath(os.path.join(site_root, pf))
                                      for pf in preserve_files)

                    if should_save:
                        target_tmp_file = os.path.join(preserved_tmp_base, rel_path, filename)
                        os.makedirs(os.path.dirname(target_tmp_file), exist_ok=True)
                        copy2(file_abs_path, target_tmp_file)

        # Шаг 2: Удаление ТОЛЬКО содержимого целевых папок
        print("\n=== ЭТАП 2: Очистка содержимого целевых папок ===")
        for root_folder in folders_to_clean:
            norm_root = os.path.normpath(root_folder)

            if not os.path.exists(norm_root):
                os.makedirs(norm_root, exist_ok=True)
                if uid is not None:
                    apply_permissions(norm_root, uid, gid)
                continue

            # Получаем список того, что нужно удалить внутри папки
            items_inside = os.listdir(norm_root)
            for item in items_inside:
                item_path = os.path.join(norm_root, item)

                skip_item = False
                # Проверяем защищенные подпапки
                for preserve_rel in preserve_dirs:
                    full_preserve_abs = os.path.normpath(os.path.join(site_root, preserve_rel))
                    # Если это сама папка или она находится ВНУТРИ сохраняемой
                    if item_path == full_preserve_abs or item_path.startswith(full_preserve_abs + os.sep):
                        skip_item = True
                        break

                # Проверяем защищенные файлы
                if not skip_item:
                    for preserve_rel in preserve_files:
                        full_preserve_abs = os.path.normpath(os.path.join(site_root, preserve_rel))
                        if item_path == full_preserve_abs:
                            skip_item = True
                            break

                if not skip_item:
                    if os.path.isfile(item_path) or os.path.islink(item_path):
                        os.unlink(item_path)
                        print(f"Удален файл/ссылка: {item_path}")
                    elif os.path.isdir(item_path):
                        rmtree(item_path)
                        print(f"Удалена папка: {item_path}")

        # Шаг 3: Возвращаем исключения назад
        print("\n=== ЭТАП 3: Восстановление исключений ===")
        if os.path.exists(preserved_tmp_base):
            for root_folder in folders_to_clean:
                copytree(preserved_tmp_base, root_folder, dirs_exist_ok=True)
                print(f"Исключения возвращены в {root_folder}")

        # Шаг 4: Назначение прав на ВСЕ созданные/очищенные папки
        print("\n=== ЭТАП 4: Назначение прав доступа ===")
        if uid is not None:
            for root_folder in folders_to_clean:
                apply_permissions(root_folder, uid, gid)

        # Шаг 5: Копирование данных из секции copy_sources
        print("\n=== ЭТАП 5: Копирование дополнительных файлов ===")
        copy_files_to_cleaned_folders(settings)

        # Шаг 6: Финальная чистка временных файлов
        if os.path.exists(tmp_dir):
            rmtree(tmp_dir)

    except Exception as e:
        print(f"Критическая ошибка при обработке папок: {str(e)}")

def copy_files_to_cleaned_folders(settings):
    """Копирование файлов и папок в очищенные директории"""
    folders_settings = settings['folders']
    copy_sources = folders_settings['copy_sources']
    copy_destinations = folders_settings['copy_destinations']
    folders_to_clean = folders_settings['clean']
    copy_user = folders_settings['copy_user']

    if not copy_sources or not copy_destinations:
        print("Не указаны папки для копирования")
        return

    if len(copy_sources) != len(copy_destinations):
        print("Количество исходных и целевых папок для копирования не совпадает")
        return

    for i, source_folder in enumerate(copy_sources):
        dest_folder = copy_destinations[i]

        if dest_folder not in folders_to_clean:
            print(f"Предупреждение: целевая папка {dest_folder} не была в списке очищаемых")
            continue

        try:
            if not os.path.exists(source_folder):
                print(f"Исходная папка не существует: {source_folder}")
                continue

            if not os.path.exists(dest_folder):
                print(f"Создаем целевую папку: {dest_folder}")
                if copy_user:
                    success, output = run_as_user(['mkdir', '-p', dest_folder], copy_user)
                    if not success:
                        print(f"Ошибка создания папки: {output}")
                        continue
                else:
                    os.makedirs(dest_folder, exist_ok=True)

            print(f"Копирование из {source_folder} в {dest_folder}")

            if os.path.isdir(source_folder):
                for item in os.listdir(source_folder):
                    source_path = os.path.join(source_folder, item)
                    dest_path = os.path.join(dest_folder, item)

                    if copy_user:
                        if os.path.isdir(source_path):
                            success, output = run_as_user(['rsync', '-ar', source_path + '/', dest_path + '/'],
                                                          copy_user)
                        else:
                            success, output = run_as_user(['cp', '-p', source_path, dest_path], copy_user)

                        if success:
                            print(f"  Скопировано: {item}")
                        else:
                            print(f"  Ошибка копирования {item}: {output}")
                    else:
                        if os.path.isdir(source_path):
                            if os.path.exists(dest_path):
                                rmtree(dest_path)
                            copytree(source_path, dest_path)
                            print(f"  Скопирована папка: {item}")
                        else:
                            copy2(source_path, dest_path)
                            print(f"  Скопирован файл: {item}")

            print(f"Копирование в {dest_folder} завершено")

        except Exception as e:
            print(f"Ошибка при копировании из {source_folder} в {dest_folder}: {str(e)}")

def log_db_credentials_safe(db_settings):
    print("\n=== ПРОВЕРКА УЧЕТНЫХ ДАННЫХ БД ===")
    print(f"[OK] Хост: {db_settings['host']}")
    print(f"[OK] Пользователь: {db_settings['user']}")
    print(f"[OK] База данных: {db_settings['database_name']}")
    print("[INFO] Пароль успешно загружен в память")
    print("=" * 36)

def show_database_info(settings):
    """Показать информацию о текущем состоянии базы данных"""
    db_settings = settings['database']
    connection, error = create_db_connection(db_settings)
    if error:
        print(error)
        return

    try:
        tables = get_all_tables(connection)
        print(f"Текущее количество таблиц в базе '{db_settings['database_name']}': {len(tables)}")
        if tables:
            print("Список таблиц:", ", ".join(tables))
        else:
            print("В базе данных нет таблиц")

    except Exception as e:
        print(f"Ошибка при получении информации о БД: {str(e)}")
    finally:
        if connection:
            connection.close()


def create_backup(settings, source_folders):
    """Создание резервной копии файлов перед очисткой"""
    if not settings['backup']['enable']:
        print("Резервное копирование отключено в настройках")
        return None

    backup_dir = settings['backup']['backup_dir']
    if not backup_dir:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = f"/tmp/backup_{timestamp}"

    try:
        os.makedirs(backup_dir, exist_ok=True)
        print(f"Создание резервной копии в {backup_dir}")

        for source_folder in source_folders:
            if os.path.exists(source_folder):
                folder_name = os.path.basename(source_folder.rstrip('/'))
                backup_path = os.path.join(backup_dir, folder_name)

                if os.path.isdir(source_folder):
                    copytree(source_folder, backup_path)
                    print(f"  Резервная копия создана: {source_folder} -> {backup_path}")
                else:
                    copy2(source_folder, backup_path)
                    print(f"  Резервная копия создана: {source_folder} -> {backup_path}")

        print("Резервное копирование завершено")
        return backup_dir

    except Exception as e:
        print(f"Ошибка при создании резервной копии: {str(e)}")
        return None


def print_settings_summary(settings):
    """Вывод сводки настроек"""
    print("=== ЗАГРУЖЕННЫЕ НАСТРОЙКИ ===")
    print(f"База данных: {settings['database']['database_name']}")
    print(f"Режим БД: {settings['database']['mode']}")
    if settings['database']['tables']:
        print(f"Таблицы для обработки: {', '.join(settings['database']['tables'])}")

    print(f"Папки для очистки: {', '.join(settings['folders']['clean'])}")

    if settings['folders']['copy_sources']:
        print(f"Источники для копирования: {', '.join(settings['folders']['copy_sources'])}")
        print(f"Назначения для копирования: {', '.join(settings['folders']['copy_destinations'])}")
        if settings['folders']['copy_user']:
            print(f"Пользователь для копирования: {settings['folders']['copy_user']}")

    if settings['folders']['preserve_dirs']:
        print(f"Сохраняемые подпапки: {', '.join(settings['folders']['preserve_dirs'])}")
    if settings['folders']['preserve_files']:
        print(f"Сохраняемые файлы: {', '.join(settings['folders']['preserve_files'])}")

    print(f"Резервное копирование: {'включено' if settings['backup']['enable'] else 'отключено'}")
    if settings['backup']['enable'] and settings['backup']['backup_dir']:
        print(f"Папка для бэкапов: {settings['backup']['backup_dir']}")

    print(
        f"Подтверждение операций: {'включено' if settings['security']['confirm_destructive_operations'] else 'отключено'}")
    print("=" * 30)


def check_dependencies():
    """Проверка необходимых зависимостей"""
    try:
        import pymysql
        import cryptography
        return True, None
    except ImportError as e:
        missing_package = str(e).split(" ")[-1]
        return False, f"Не установлен пакет: {missing_package}\nУстановите: pip3 install {missing_package}"


if __name__ == "__main__":
    try:
        deps_ok, deps_error = check_dependencies()
        if not deps_ok:
            print(f"Ошибка зависимостей: {deps_error}")
            sys.exit(1)

        settings = load_settings()

        print("Начало очистки...")
        print_settings_summary(settings)

        log_db_credentials_safe(settings['database'])

        if settings['backup']['enable']:
            print("\n=== Резервное копирование ===")
            backup_folders = list(set(settings['folders']['clean'] + settings['folders']['copy_sources']))
            backup_path = create_backup(settings, backup_folders)
        else:
            backup_path = None
            print("\nРезервное копирование отключено в настройках")

        print("\n=== Состояние БД ДО очистки ===")
        show_database_info(settings)

        db_settings = settings['database']

        print(f"\n=== Работа с базой данных (режим: {db_settings.get('mode', 'не указан')}) ===")

        # Явно выводим параметры для проверки глазами
        print(f"[DEBUG] Target DB Name: '{db_settings.get('database_name')}'")
        print(f"[DEBUG] Mode selected: '{db_settings.get('mode')}'")
        print(f"[DEBUG] Tables list: {db_settings.get('tables')}")

        if not db_settings.get('database_name'):
            print("[ERROR] Имя базы данных ('database_name') пустое. Очистка БД пропущена.")
        else:
            # Вызываем функцию очистки только если имя БД задано
            clean_database(settings)

        print("\n=== Состояние БД ПОСЛЕ очистки ===")
        show_database_info(settings)

        if settings['folders']['clean']:
            print("\n=== Очистка файловой системы ===")
            clean_folders(settings['folders']['clean'], settings)
        else:
            print("Нет папок для очистки")

        if settings['folders']['copy_sources'] and settings['folders']['copy_destinations']:
            print("\n=== Копирование файлов в очищенные папки ===")
            copy_files_to_cleaned_folders(settings)
        else:
            print("Нет файлов для копирования")

        print("\nОчистка и копирование завершены")
        if backup_path:
            print(f"Резервная копия создана в: {backup_path}")

    except FileNotFoundError as e:
        print(f"Ошибка: {e}")
        print("Создайте файл settings.ini на основе примера")
    except Exception as e:
        print(f"Критическая ошибка: {e}")