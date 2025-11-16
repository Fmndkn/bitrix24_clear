#!/usr/bin/env python3

import os
import pymysql
import shutil
import configparser
import subprocess
import getpass
import sys
import re
from datetime import datetime
from shutil import rmtree, copytree, copy2


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

    # Регулярное выражение для поиска значений в кавычках или без
    pattern = r'\"[^\"]+\"|\'[^\']+\'|[^,\s]+'
    matches = re.findall(pattern, value)

    result = []
    for match in matches:
        # Удаляем кавычки если они есть
        cleaned_value = unquote_value(match)
        if cleaned_value:
            result.append(cleaned_value)

    return result


def load_settings():
    """Загрузка настроек из settings.ini"""
    config = configparser.ConfigParser(interpolation=None)

    # Читаем файл настроек
    if not os.path.exists('settings.ini'):
        raise FileNotFoundError("Файл настроек settings.ini не найден")

    config.read('settings.ini', encoding='utf-8')

    settings = {
        'database': {
            'host': unquote_value(config.get('database', 'host', fallback='localhost')),
            'user': unquote_value(config.get('database', 'user', fallback='root')),
            'password': unquote_value(config.get('database', 'password', fallback='')),
            'database_name': unquote_value(config.get('database', 'database_name', fallback='')),
            'mode': unquote_value(config.get('database', 'mode', fallback='truncate')),
            'tables': parse_quoted_list(config.get('database', 'tables', fallback='')),
            'auth_plugin': unquote_value(config.get('database', 'auth_plugin', fallback=None))
        },
        'folders': {
            'clean': parse_quoted_list(config.get('folders', 'clean', fallback='')),
            'copy_sources': parse_quoted_list(config.get('folders', 'copy_sources', fallback='')),
            'copy_destinations': parse_quoted_list(config.get('folders', 'copy_destinations', fallback='')),
            'copy_user': unquote_value(config.get('folders', 'copy_user', fallback=''))
        },
        'backup': {
            'enable': config.getboolean('backup', 'enable', fallback=True),
            'backup_dir': unquote_value(config.get('backup', 'backup_dir', fallback=''))
        },
        'security': {
            'confirm_destructive_operations': config.getboolean('security', 'confirm_destructive_operations',
                                                                fallback=True)
        }
    }

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
            # Временно отключаем проверку внешних ключей
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")

            # Получаем все таблицы
            tables = get_all_tables(connection)

            if not tables:
                print("В базе данных нет таблиц для удаления")
                return

            print(f"Найдено таблиц для удаления: {len(tables)}")

            # Формируем и выполняем запрос на удаление всех таблиц
            drop_query = "DROP TABLE " + ", ".join(tables) + ";"
            cursor.execute(drop_query)

            # Включаем проверку внешних ключей обратно
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")

            print(f"Все таблицы ({len(tables)}) успешно удалены")

    except Exception as e:
        print(f"Ошибка при удалении таблиц: {str(e)}")
        # Все равно включаем проверку внешних ключей при ошибке
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
            print(f"Указанные таблицы ({len(tables_list)}) успешно удалены")

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
    """Очистка указанных папок"""
    if settings['security']['confirm_destructive_operations'] and folders_to_clean:
        if not confirm_destructive_operation(f"Будет очищено содержимое папок: {', '.join(folders_to_clean)}"):
            print("Операция отменена пользователем")
            return

    for folder in folders_to_clean:
        try:
            if os.path.exists(folder):
                for item in os.listdir(folder):
                    item_path = os.path.join(folder, item)
                    if os.path.isfile(item_path):
                        os.unlink(item_path)
                        print(f"Удален файл: {item_path}")
                    elif os.path.isdir(item_path):
                        rmtree(item_path)
                        print(f"Удалена папка: {item_path}")
                print(f"Папка {folder} очищена")
            else:
                print(f"Папка не существует: {folder}")
        except Exception as e:
            print(f"Ошибка при очистке {folder}: {str(e)}")


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

        # Проверяем, что целевая папка была в списке очищаемых
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

            # Копируем содержимое папки
            if os.path.isdir(source_folder):
                for item in os.listdir(source_folder):
                    source_path = os.path.join(source_folder, item)
                    dest_path = os.path.join(dest_folder, item)

                    if copy_user:
                        # Копирование от имени другого пользователя
                        if os.path.isdir(source_path):
                            # Для папок используем rsync для сохранения прав
                            success, output = run_as_user(['rsync', '-ar', source_path + '/', dest_path + '/'],
                                                          copy_user)
                        else:
                            # Для файлов используем cp
                            success, output = run_as_user(['cp', '-p', source_path, dest_path], copy_user)

                        if success:
                            print(f"  Скопировано: {item}")
                        else:
                            print(f"  Ошибка копирования {item}: {output}")
                    else:
                        # Обычное копирование
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
        # Проверяем зависимости
        deps_ok, deps_error = check_dependencies()
        if not deps_ok:
            print(f"Ошибка зависимостей: {deps_error}")
            sys.exit(1)

        # Загружаем настройки
        settings = load_settings()

        print("Начало очистки...")
        print_settings_summary(settings)

        # Создаем резервную копию перед очисткой
        if settings['backup']['enable']:
            print("\n=== Резервное копирование ===")
            backup_folders = list(set(settings['folders']['clean'] + settings['folders']['copy_sources']))
            backup_path = create_backup(settings, backup_folders)
        else:
            backup_path = None
            print("\nРезервное копирование отключено в настройках")

        # Показываем информацию о БД до очистки
        print("\n=== Состояние БД ДО очистки ===")
        show_database_info(settings)

        # Работа с базой данных
        if settings['database']['database_name']:
            print(f"\n=== Работа с базой данных (режим: {settings['database']['mode']}) ===")
            clean_database(settings)
        else:
            print("Имя базы данных не указано, пропускаем очистку БД")

        # Показываем информацию о БД после очистки
        print("\n=== Состояние БД ПОСЛЕ очистки ===")
        show_database_info(settings)

        # Очистка файлов
        if settings['folders']['clean']:
            print("\n=== Очистка файловой системы ===")
            clean_folders(settings['folders']['clean'], settings)
        else:
            print("Нет папок для очистки")

        # Копирование файлов в очищенные папки
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