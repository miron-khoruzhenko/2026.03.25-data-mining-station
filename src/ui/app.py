import streamlit as st
import sys
import time
import json
import pandas as pd
from pathlib import Path
import os 
import threading

# Подключение модулей из корня проекта
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

from src.db.db_manager import DBManager
from src.modes.crawler import CategoryCrawler
from src.modes.scraper import DataScraper
from src.utils.exporter import ExcelExporter

st.set_page_config(page_title="Strewen - AI Data Miner", page_icon="⛏️", layout="wide")

# ================= ЭКРАН АВТОРИЗАЦИИ =================
def check_password():
    """Возвращает True, если введен правильный пароль."""
    CORRECT_PASSWORD = "135531"

    if st.session_state.get("password_correct", False):
        return True

    # Центрируем форму ввода пароля
    col1, col2, col3 = st.columns([1, 1.5, 1])
    with col2:
        st.write("<br><br><br>", unsafe_allow_html=True)
        with st.form("login_form"):
            st.markdown("<h2 style='text-align: center;'>⛏️ AI Data Miner</h2>", unsafe_allow_html=True)
            st.markdown("<p style='text-align: center; color: gray;'>Защищенная панель управления</p>", unsafe_allow_html=True)
            
            pwd = st.text_input("Пароль", type="password", label_visibility="collapsed", placeholder="Введите пароль...")
            
            if st.form_submit_button("Войти в систему", type="primary", width='stretch'):
                if pwd == CORRECT_PASSWORD:
                    st.session_state["password_correct"] = True
                    st.rerun()
                else:
                    st.error("❌ Неверный пароль")
    return False

if not check_password():
    st.stop()
# =====================================================

@st.cache_resource
def get_db():
    return DBManager()

db = get_db()

def load_stats():
    with db.get_connection() as conn:
        tasks = conn.execute("SELECT status, COUNT(*) as cnt FROM crawler_tasks GROUP BY status").fetchall()
        items = conn.execute("SELECT status, COUNT(*) as cnt FROM scraper_items GROUP BY status").fetchall()
        
        stats = {
            'crawler': {'pending': 0, 'done': 0, 'error': 0},
            'scraper': {'pending': 0, 'processing': 0, 'done': 0, 'error': 0, 'empty': 0}
        }
        for row in tasks:
            stats['crawler'][row['status']] = row['cnt']
        for row in items:
            stats['scraper'][row['status']] = row['cnt']
            
        return stats

def format_time(seconds):
    """Форматирует секунды в MM:SS или HH:MM:SS"""
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}" if h > 0 else f"{m:02d}:{s:02d}"

# --- UI Layout ---

st.title("⛏️ AI Data Miner Dashboard")

stats = load_stats()
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Очередь Категорий", stats['crawler']['pending'])
with col2:
    st.metric("Очередь Карточек", stats['scraper']['pending'])
with col3:
    st.metric("Успешно собрано", stats['scraper']['done'])
with col4:
    st.metric("Пустых/Ошибок", stats['scraper'].get('error', 0) + stats['scraper'].get('empty', 0))

st.divider()

# --- САЙДБАР ---
with st.sidebar:
    st.header("⚙️ Управление задачами")
    
    st.subheader("📥 Импорт ссылок")
    
    # Меняем порядок вкладок: Карточки теперь первые в списке
    tab_imp_card, tab_imp_cat = st.tabs(["В Карточки", "В Категории"])
    
    with tab_imp_card:
        links_input_card = st.text_area("Ссылки на карточки:", height=100, key="input_card")
        category_name = st.text_input("Имя категории (для Excel листа):", placeholder="Оставь пустым для 'manual_import'")
        
        if st.button("Добавить в Карточки", width='stretch', type="primary", key="btn_card"):
            if links_input_card.strip():
                links = [line.strip() for line in links_input_card.split('\n') if line.strip()]
                save_category = category_name.strip() if category_name.strip() else "manual_import"
                db.add_scraper_items(source_url=save_category, data_urls=links)
                st.success(f"Добавлено {len(links)} шт. в '{save_category}'")
                st.rerun()

    with tab_imp_cat:
        links_input_cat = st.text_area("Ссылки на списки:", height=100, key="input_cat")
        if st.button("Добавить в очередь", width='stretch', key="btn_cat"):
            if links_input_cat.strip():
                links = [line.strip() for line in links_input_cat.split('\n') if line.strip()]
                for link in links: db.add_category_task(link)
                st.success(f"Добавлено {len(links)} шт.")
                st.rerun()

    st.divider()
    
    st.subheader("Настройки парсинга")
    custom_fields_input = st.text_area("Кастомные поля (через запятую)", value="ofertant_name, ofertant_cif, autoritate_name, autoritate_cif")
    use_only_custom = st.checkbox("Искать ТОЛЬКО кастомные поля", value=True)
    is_headless = st.checkbox("Скрытый режим (Headless)", value=True, help="Ускоряет работу, но повышает риск блокировки.")
    
    st.divider()
    
    st.subheader("💾 Экспорт в Excel")
    export_name = st.text_input("Префикс файла", value="dataset")
    
    if st.button("⚙️ Сгенерировать Excel", use_container_width=True):
        with st.spinner("Формируем таблицу..."):
            exporter = ExcelExporter()
            path = exporter.export_done_items(export_name)
            if path: 
                st.session_state['ready_export_path'] = str(path)
                st.success("Готово! Нажми 'Скачать' ниже.")
            else: 
                st.warning("Нет данных для экспорта.")
                st.session_state['ready_export_path'] = None

    # Проверяем, готов ли файл к скачиванию
    export_file_path = st.session_state.get('ready_export_path')
    is_file_ready = bool(export_file_path and os.path.exists(export_file_path))

    # Готовим данные (если файла нет, отдаем пустые байты, чтобы кнопка не сломалась)
    if is_file_ready:
        with open(export_file_path, "rb") as f:
            file_data = f.read()
        download_name = os.path.basename(export_file_path)
    else:
        file_data = b""
        download_name = "empty.xlsx"

    # Кнопка скачивания видна всегда, но активна только когда есть файл
    st.download_button(
        label="⬇️ Скачать на компьютер",
        data=file_data,
        file_name=download_name,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        type="primary",
        disabled=not is_file_ready
    )

    st.divider()

    st.subheader("🗄️ Резервная копия БД")
    db.force_checkpoint()
    with open(db.db_path, "rb") as f:
        st.download_button(
            label="💾 Скачать всю базу (.db)",
            data=f,
            file_name="mining_state_backup.db",
            mime="application/octet-stream",
            width='stretch'
        )
    
    uploaded_db = st.file_uploader("📂 Восстановить базу", type=["db"])
    if uploaded_db is not None:
        if st.button("⚠️ Перезаписать текущую базу", type="primary", width='stretch'):
            with open(db.db_path, "wb") as f:
                f.write(uploaded_db.getbuffer())
            st.success("База успешно восстановлена!")
            st.rerun()

    st.divider()

    # Защищенная кнопка очистки
    if "confirm_clear" not in st.session_state:
        st.session_state.confirm_clear = False

    if not st.session_state.confirm_clear:
        if st.button("🗑 Очистить все очереди", width='stretch'):
            st.session_state.confirm_clear = True
            st.rerun()
    else:
        st.warning("⚠️ Точно удалить все данные? (Кэш AI останется)")
        col_y, col_n = st.columns(2)
        if col_y.button("Да, удалить", type="primary", width='stretch'):
            db.clear_all_queues()
            st.session_state.confirm_clear = False
            st.rerun()
        if col_n.button("Отмена", width='stretch'):
            st.session_state.confirm_clear = False
            st.rerun()


# --- ОСНОВНАЯ ПАНЕЛЬ ---
tab1, tab2, tab3 = st.tabs(["🚀 Запуск", "🧠 Кэш AI", "📋 Управление данными"])

with tab1:
    col_run1, col_run2 = st.columns(2)
    
    with col_run1:
        st.subheader("Режим 1: Crawler")
        pages_to_crawl = st.number_input("Страниц пагинации за запуск?", min_value=1, max_value=999999, value=99999)
        if st.button("▶️ Запустить Crawler", type="primary", width='stretch'):
            st.markdown("### 🔄 Лайв статус работы")
            live_logs = st.empty()
            log_history = []

            def crawler_callback(msg, stats=None):
                if msg:
                    log_history.append(msg)
                    live_logs.code("\n".join(log_history[-15:]), language="text")

            with st.spinner("Работает... Для экстренной остановки нажми 'Stop' в правом верхнем углу."):
                CategoryCrawler().run(max_pages_to_test=pages_to_crawl, headless=is_headless, ui_callback=crawler_callback)
            st.rerun()

    with col_run2:
        st.subheader("Режим 2: Scraper")
        items_to_scrape = st.number_input("Карточек за запуск?", min_value=1, max_value=999999, value=99999)
        
        # 1. Проверяем все активные потоки сервера
        is_scraper_running = any(t.name == "ScraperBackgroundThread" for t in threading.enumerate())
        
        # 2. Меняем интерфейс в зависимости от статуса
        if is_scraper_running:
            st.warning("⏳ Скрейпер в данный момент работает в фоновом режиме.")
            st.button("▶️ Запустить Scraper", disabled=True, use_container_width=True)
            st.info("💡 Обновляй страницу (F5), чтобы видеть, как растут счетчики сверху. Вкладку можно безопасно закрыть.")
        else:
            if st.button("▶️ Запустить Scraper (в фоне)", type="primary", use_container_width=True):
                
                custom_fields = [f.strip() for f in custom_fields_input.split(",") if f.strip()]
                
                def background_scraper():
                    try:
                        DataScraper().run(
                            custom_fields=custom_fields, 
                            max_items_to_test=items_to_scrape, 
                            use_only_custom=use_only_custom,
                            headless=is_headless,
                            ui_callback=None 
                        )
                    except Exception as e:
                        print(f"[!] Ошибка фонового потока: {e}")

                # Создаем поток с УНИКАЛЬНЫМ ИМЕНЕМ
                thread = threading.Thread(target=background_scraper, name="ScraperBackgroundThread", daemon=True)
                thread.start()
                
                # Даем потоку полсекунды на старт, затем перезагружаем интерфейс для смены кнопки
                time.sleep(0.5)
                st.rerun()
          
            
with tab2:
    st.subheader("Управление селекторами (ai_configs)")
    st.info("Используй Raw JSON для массовой замены селекторов (копируй в VS Code, редактируй через Ctrl+D и возвращай обратно).")
    
    with db.get_connection() as conn:
        configs = pd.read_sql_query("SELECT domain, selectors_json FROM ai_configs", conn)
    
    if not configs.empty:
        for idx, row in configs.iterrows():
            domain = row['domain']
            with st.expander(f"🌐 Домен: {domain}", expanded=False):
                selectors_dict = json.loads(row['selectors_json'])
                
                edit_tab1, edit_tab2 = st.tabs(["🗂️ Таблица", "📝 Raw JSON (Код)"])
                
                with edit_tab1:
                    clean_dict = {k: (v if v is not None else "") for k, v in selectors_dict.items()}
                    df = pd.DataFrame(list(clean_dict.items()), columns=['Поле', 'Селектор'])
                    
                    edited_df = st.data_editor(
                        df, 
                        key=f"editor_tbl_{domain}", 
                        width='stretch',
                        disabled=["Поле"],
                        hide_index=True
                    )
                    
                    if st.button(f"💾 Сохранить таблицу", key=f"save_tbl_{domain}", type="primary"):
                        new_selectors = {}
                        for _, r in edited_df.iterrows():
                            val = r['Селектор']
                            if isinstance(val, str) and val.strip() != "":
                                new_selectors[r['Поле']] = val.strip()
                            else:
                                new_selectors[r['Поле']] = None
                                
                        db.update_ai_config(domain, new_selectors)
                        st.success("Таблица успешно сохранена!")
                        st.rerun()
                        
                with edit_tab2:
                    raw_json_str = json.dumps(selectors_dict, indent=4, ensure_ascii=False)
                    edited_json_str = st.text_area(
                        "Редактор JSON", 
                        value=raw_json_str, 
                        height=400, 
                        key=f"editor_raw_{domain}"
                    )
                    
                    if st.button(f"💾 Сохранить JSON", key=f"save_raw_{domain}", type="primary"):
                        try:
                            parsed_json = json.loads(edited_json_str)
                            db.update_ai_config(domain, parsed_json)
                            st.success("JSON успешно сохранен!")
                            st.rerun()
                        except json.JSONDecodeError as e:
                            st.error(f"Ошибка синтаксиса JSON: {e}")

                st.divider()
                if st.button(f"🗑️ Удалить кэш домена", key=f"del_cache_{domain}"):
                    db.delete_ai_config(domain)
                    st.rerun()
    else:
        st.write("Кэш селекторов пока пуст.")
        
        
# === ВКЛАДКА УПРАВЛЕНИЯ ДАННЫМИ С ПАГИНАЦИЕЙ ===
with tab3:
    col_t1, col_t2 = st.columns([3, 1])
    with col_t1:
        st.subheader("Управление записями (Scraper Items)")
    with col_t2:
        if st.button("🔄 Вернуть все пустые в очередь", type="secondary", width='stretch'):
            db.requeue_all_empty_and_errors()
            st.success("Все пустые строки снова получили статус 'pending'!")
            st.rerun()

    st.info("Выбери строки галочками, чтобы удалить их или отправить на повторный парсинг.")
    
    # --- ЛОГИКА ПАГИНАЦИИ ---
    ROWS_PER_PAGE = 500
    
    if "current_page" not in st.session_state:
        st.session_state.current_page = 1

    with db.get_connection() as conn:
        # Узнаем общее количество строк
        total_rows = conn.execute("SELECT COUNT(*) FROM scraper_items").fetchone()[0]
        total_pages = max(1, (total_rows + ROWS_PER_PAGE - 1) // ROWS_PER_PAGE)
        
        # Корректировка страницы, если удалили данные и вышли за пределы
        if st.session_state.current_page > total_pages:
            st.session_state.current_page = total_pages

        # Вычисляем сдвиг (OFFSET)
        offset = (st.session_state.current_page - 1) * ROWS_PER_PAGE
        
        # Загружаем только нужный кусок
        latest_data = pd.read_sql_query(
            f"SELECT id, data_url, status, extracted_data FROM scraper_items ORDER BY id DESC LIMIT {ROWS_PER_PAGE} OFFSET {offset}", 
            conn
        )
    
    # --- Отрисовка кнопок пагинации (ВЕРХ) ---
    col_p1, col_p2, col_p3 = st.columns([1, 2, 1])
    with col_p1:
        if st.button("⬅️ Предыдущая", disabled=(st.session_state.current_page == 1), width='stretch'):
            st.session_state.current_page -= 1
            st.rerun()
    with col_p2:
        st.markdown(f"<div style='text-align: center; margin-top: 8px;'><b>Страница {st.session_state.current_page} из {total_pages}</b> (Всего записей: {total_rows})</div>", unsafe_allow_html=True)
    with col_p3:
        if st.button("Следующая ➡️", disabled=(st.session_state.current_page == total_pages), width='stretch'):
            st.session_state.current_page += 1
            st.rerun()
    
    # --- Отрисовка таблицы ---
    if not latest_data.empty:
        expanded_rows = []
        all_keys = set()
        for _, row in latest_data.iterrows():
            if row['extracted_data']:
                try:
                    data = json.loads(row['extracted_data'])
                    all_keys.update(data.keys())
                except: pass

        for _, row in latest_data.iterrows():
            base = {'id': row['id'], 'status': row['status'], 'url': row['data_url']}
            for k in all_keys: base[k] = None
            
            if row['extracted_data']:
                try:
                    data = json.loads(row['extracted_data'])
                    base.update(data)
                except: pass
            expanded_rows.append(base)
            
        df = pd.DataFrame(expanded_rows)
        df.insert(0, "Select", False)
        
        # Конфигурация специфичных колонок
        col_config = {
            "Select": st.column_config.CheckboxColumn("Выбрать", default=False),
            "url": st.column_config.LinkColumn("Ссылка", display_text="Открыть карточку ↗") 
        }
        
        edited_df = st.data_editor(
            df,
            hide_index=True,
            column_config=col_config,
            disabled=[col for col in df.columns if col != "Select"],
            width='stretch',
            height=600
        )

        selected_rows = edited_df[edited_df["Select"] == True]
        selected_ids = selected_rows["id"].tolist()
        
        if selected_ids:
            st.warning(f"Выбрано строк на этой странице: {len(selected_ids)}")
            col_act1, col_act2 = st.columns(2)
            
            with col_act1:
                if st.button("🔄 Повторить парсинг (Pending)", type="primary", width='stretch'):
                    db.requeue_items(selected_ids)
                    st.success("Строки возвращены в очередь!")
                    st.rerun()
                    
            with col_act2:
                if st.button("🗑️ Удалить навсегда", type="primary", width='stretch'):
                    db.delete_scraper_items(selected_ids)
                    st.success("Строки удалены!")
                    st.rerun()
    else:
        st.write("База данных пока пуста.")