import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import sys
import os
import logging
from datetime import datetime
import json

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('app_debug.log')
    ]
)
logger = logging.getLogger(__name__)

from api_logic import fetch_report, get_available_metrics, save_to_csv, ATTRIBUTION_TYPES
from datetime import timedelta

class MetrikaApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Metrika Logs Downloader")
        
        # Путь к файлу настроек
        self.settings_file = os.path.join(os.path.dirname(__file__), "settings.json")
        
        # Создаем контекстное меню
        self.create_context_menu()
        
        # Настройка главного окна для центрирования
        root.grid_rowconfigure(0, weight=1)
        root.grid_columnconfigure(0, weight=1)
        
        # Основной контейнер для всех элементов
        main_container = ttk.Frame(root, padding="10")
        main_container.grid(row=0, column=0, sticky='nsew')
        main_container.grid_columnconfigure(0, weight=1)
        
        # Frame для полей ввода
        input_frame = ttk.Frame(main_container, padding="5")
        input_frame.grid(row=0, column=0, padx=10, pady=5, sticky='ew')
        input_frame.grid_columnconfigure(1, weight=1)
        
        # Fixed width for labels
        label_width = 15
        entry_width = 50
        
        # OAuth Token
        ttk.Label(input_frame, text="OAuth Token:", width=label_width, anchor='e').grid(
            row=0, column=0, padx=5, pady=5, sticky='e')
        self.token_entry = ttk.Entry(input_frame, width=entry_width)
        self.token_entry.grid(row=0, column=1, columnspan=2, padx=5, pady=5, sticky='ew')
        self.bind_widget_events(self.token_entry)

        # Counter ID
        ttk.Label(input_frame, text="Counter ID:", width=label_width, anchor='e').grid(
            row=1, column=0, padx=5, pady=5, sticky='e')
        self.counter_entry = ttk.Entry(input_frame, width=entry_width)
        self.counter_entry.grid(row=1, column=1, columnspan=2, padx=5, pady=5, sticky='ew')
        self.bind_widget_events(self.counter_entry)

        # Login
        ttk.Label(input_frame, text="Login:", width=label_width, anchor='e').grid(
            row=2, column=0, padx=5, pady=5, sticky='e')
        self.login_entry = ttk.Entry(input_frame, width=entry_width)
        self.login_entry.grid(row=2, column=1, columnspan=2, padx=5, pady=5, sticky='ew')
        self.bind_widget_events(self.login_entry)

        # Date fields
        ttk.Label(input_frame, text="Дата начала:", width=label_width, anchor='e').grid(
            row=3, column=0, padx=5, pady=5, sticky='e')
        self.date1_entry = ttk.Entry(input_frame, width=20)
        self.date1_entry.insert(0, "today")
        self.date1_entry.grid(row=3, column=1, padx=5, pady=5, sticky='w')
        self.bind_widget_events(self.date1_entry)

        ttk.Label(input_frame, text="Дата окончания:", width=label_width, anchor='e').grid(
            row=4, column=0, padx=5, pady=5, sticky='e')
        self.date2_entry = ttk.Entry(input_frame, width=20)
        self.date2_entry.insert(0, "today")
        self.date2_entry.grid(row=4, column=1, padx=5, pady=5, sticky='w')
        self.bind_widget_events(self.date2_entry)

        # Report type
        ttk.Label(input_frame, text="Тип отчета:", width=label_width, anchor='e').grid(
            row=5, column=0, padx=5, pady=5, sticky='e')
        self.report_type = ttk.Combobox(
            input_frame,
            values=['visits', 'hits'],
            state='readonly',
            width=18
        )
        self.report_type.set('visits')
        self.report_type.grid(row=5, column=1, padx=5, pady=5, sticky='w')
        self.report_type.bind('<<ComboboxSelected>>', self.on_report_type_change)

        # Frame для настроек отчета
        report_frame = ttk.LabelFrame(main_container, text="Настройки отчета", padding=5)
        report_frame.grid(row=1, column=0, padx=10, pady=5, sticky='ew')
        report_frame.grid_columnconfigure(0, weight=1)
        
        # Frame для метрик
        metrics_frame = ttk.Frame(report_frame)
        metrics_frame.grid(row=0, column=0, padx=5, pady=5, sticky='ew')
        metrics_frame.grid_columnconfigure((0, 2), weight=1)  # Равное распределение для списков
        
        # Frame для доступных метрик
        left_frame = ttk.LabelFrame(metrics_frame, text="Доступные метрики")
        left_frame.grid(row=0, column=0, padx=5, sticky='ew')
        left_frame.grid_columnconfigure(0, weight=1)
        
        self.available_metrics = tk.Listbox(left_frame, selectmode=tk.MULTIPLE, height=10)
        scrollbar1 = ttk.Scrollbar(left_frame, orient=tk.VERTICAL)
        scrollbar1.config(command=self.available_metrics.yview)
        self.available_metrics.config(yscrollcommand=scrollbar1.set)
        self.available_metrics.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar1.pack(side=tk.RIGHT, fill=tk.Y)

        # Frame для кнопок управления метриками
        btn_frame = ttk.Frame(metrics_frame)
        btn_frame.grid(row=0, column=1, padx=10)
        
        ttk.Button(btn_frame, text=">>", 
            command=self.add_selected_metrics).pack(pady=2)
        ttk.Button(btn_frame, text="<<", 
            command=self.remove_selected_metrics).pack(pady=2)
        ttk.Button(btn_frame, text="Все >>", 
            command=self.add_all_metrics).pack(pady=2)
        ttk.Button(btn_frame, text="<< Все", 
            command=self.remove_all_metrics).pack(pady=2)

        # Frame для выбранных метрик
        right_frame = ttk.LabelFrame(metrics_frame, text="Выбранные метрики")
        right_frame.grid(row=0, column=2, padx=5, sticky='ew')
        right_frame.grid_columnconfigure(0, weight=1)
        
        self.selected_metrics = tk.Listbox(right_frame, selectmode=tk.MULTIPLE, height=10)
        scrollbar2 = ttk.Scrollbar(right_frame, orient=tk.VERTICAL)
        scrollbar2.config(command=self.selected_metrics.yview)
        self.selected_metrics.config(yscrollcommand=scrollbar2.set)
        self.selected_metrics.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar2.pack(side=tk.RIGHT, fill=tk.Y)

        # Initialize metrics for default report type
        self.update_available_metrics()

        # Frame для атрибуции и подсказок
        bottom_frame = ttk.Frame(main_container)
        bottom_frame.grid(row=2, column=0, padx=10, pady=5, sticky='ew')
        bottom_frame.grid_columnconfigure(0, weight=1)
        
        # Добавляем выбор атрибуции
        ttk.Label(bottom_frame, text="Тип атрибуции:").grid(row=0, column=0, padx=5, pady=5)
        self.attribution = ttk.Combobox(bottom_frame, width=30, state='readonly')
        self.attribution['values'] = [f"{k} - {v}" for k, v in ATTRIBUTION_TYPES.items()]
        self.attribution.set("last - Последний переход")  # значение по умолчанию
        self.attribution.grid(row=0, column=1, padx=5, pady=5)

        # Общая подсказка по датам (под полями ввода дат)
        date_hint = "Форматы дат: today, yesterday, NdaysAgo (например: 7daysAgo) или YYYY-MM-DD"
        ttk.Label(bottom_frame, text=date_hint, foreground="gray").grid(row=1, column=0, columnspan=3, padx=5, pady=2)

        # Frame для кнопок управления
        buttons_frame = ttk.Frame(main_container)
        buttons_frame.grid(row=3, column=0, pady=10, sticky='ew')
        buttons_frame.grid_columnconfigure(0, weight=1)
        
        # Центрирование кнопок в buttons_frame
        inner_buttons_frame = ttk.Frame(buttons_frame)
        inner_buttons_frame.grid(row=0, column=0)
        
        ttk.Button(inner_buttons_frame, text="Выбрать все метрики", 
                  command=self.add_all_metrics).pack(side=tk.LEFT, padx=5)
        ttk.Button(inner_buttons_frame, text="Очистить выбор", 
                  command=self.remove_all_metrics).pack(side=tk.LEFT, padx=5)
        ttk.Button(inner_buttons_frame, text="Загрузить отчет", 
                  command=self.download_report).pack(side=tk.LEFT, padx=5)

        # Загружаем сохраненные настройки
        self.load_settings()
        
        # Привязываем сохранение настроек при закрытии окна
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

    def create_context_menu(self):
        """Создание контекстного меню"""
        self.context_menu = tk.Menu(self.root, tearoff=0)
        self.context_menu.add_command(label="Вырезать", command=self.cut_text)
        self.context_menu.add_command(label="Копировать", command=self.copy_text)
        self.context_menu.add_command(label="Вставить", command=self.paste_text)
        self.context_menu.add_separator()
        self.context_menu.add_command(label="Выделить все", command=self.select_all)

    def bind_widget_events(self, widget):
        """Привязка всех событий к виджету"""
        widget.bind('<Button-3>', lambda e: self.show_context_menu(e))
        widget.bind('<Control-v>', lambda e: self.paste_text())
        widget.bind('<Control-c>', lambda e: self.copy_text())
        widget.bind('<Control-x>', lambda e: self.cut_text())
        widget.bind('<Control-a>', lambda e: self.select_all())
        # Добавляем стандартные бинды
        widget.bind('<Control-V>', lambda e: self.paste_text())
        widget.bind('<Control-C>', lambda e: self.copy_text())
        widget.bind('<Control-X>', lambda e: self.cut_text())
        widget.bind('<Control-A>', lambda e: self.select_all())

    def show_context_menu(self, event):
        """Показ контекстного меню"""
        widget = event.widget
        self.context_menu.post(event.x_root, event.y_root)

    def paste_text(self):
        try:
            self.root.focus_get().event_generate('<<Paste>>')
        except:
            pass

    def copy_text(self):
        try:
            self.root.focus_get().event_generate('<<Copy>>')
        except:
            pass

    def cut_text(self):
        try:
            self.root.focus_get().event_generate('<<Cut>>')
        except:
            pass

    def select_all(self):
        try:
            widget = self.root.focus_get()
            widget.select_range(0, tk.END)
            widget.icursor(tk.END)
        except:
            pass

    def on_report_type_change(self, event=None):
        """Обработчик смены типа отчета"""
        report_type = self.report_type.get()
        
        # Очищаем оба списка
        self.available_metrics.delete(0, tk.END)
        self.selected_metrics.delete(0, tk.END)
        
        # Получаем метрики для выбранного типа
        metrics = get_available_metrics(report_type)
        
        # Добавляем метрики в список доступных
        for metric in metrics:
            self.available_metrics.insert(tk.END, metric)
        
        logger.debug(f"Updated metrics list for {report_type}")

    def update_available_metrics(self):
        """Update available metrics list based on report type"""
        report_type = self.report_type.get()
        available = get_available_metrics(report_type)
        
        self.available_metrics.delete(0, tk.END)
        
        # Add all available metrics
        for metric in available:
            self.available_metrics.insert(tk.END, metric)
        
        logger.debug(f"Updated available metrics for {report_type}")

    def add_selected_metrics(self):
        """Добавление выбранных метрик в правый список"""
        report_type = self.report_type.get()
        prefix = "ym:s:" if report_type == "visits" else "ym:pv:"
        
        selections = self.available_metrics.curselection()
        current_selected = list(self.selected_metrics.get(0, tk.END))
        
        for index in selections:
            metric = self.available_metrics.get(index)
            # Проверяем соответствие префикса типу отчета
            if metric.startswith(prefix) and metric not in current_selected:
                self.selected_metrics.insert(tk.END, metric)
        
        logger.debug(f"Added {len(selections)} metrics to selection")

    def remove_selected_metrics(self):
        """Удалить выбранные метрики из правого списка"""
        selections = list(self.selected_metrics.curselection())
        for index in reversed(selections):
            self.selected_metrics.delete(index)
        
        logger.debug(f"Removed {len(selections)} metrics from selection")

    def add_all_metrics(self):
        """Добавление всех доступных метрик"""
        report_type = self.report_type.get()
        prefix = "ym:s:" if report_type == "visits" else "ym:pv:"
        
        available = list(self.available_metrics.get(0, tk.END))
        current_selected = list(self.selected_metrics.get(0, tk.END))
        
        # Добавляем только метрики с правильным префиксом
        for metric in available:
            if metric.startswith(prefix) and metric not in current_selected:
                self.selected_metrics.insert(tk.END, metric)
        
        logger.debug(f"Added all available {report_type} metrics")

    def remove_all_metrics(self):
        """Очистить список выбранных метрик"""
        self.selected_metrics.delete(0, tk.END)
        logger.debug("Cleared all selected metrics")

    def get_selected_metrics(self):
        """Get list of selected metrics"""
        return list(self.selected_metrics.get(0, tk.END))

    def save_report(self, data):
        """Сохранение отчета с выбором папки и имени файла"""
        try:
            if not data:
                messagebox.showwarning("Предупреждение", "Нет данных для сохранения")
                return
                
            # Получаем текущую дату для имени файла по умолчанию
            default_filename = f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
            # Открываем диалог сохранения файла
            filepath = filedialog.asksaveasfilename(
                defaultextension=".csv",
                initialfile=default_filename,
                filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
                title="Сохранить отчет как"
            )
            
            if not filepath:  # Если пользователь отменил выбор
                return
                
            # Сохраняем файл
            save_to_csv(data, filepath)
            
            # Показываем сообщение об успехе
            messagebox.showinfo(
                "Успех", 
                f"Отчет сохранен в файл:\n{filepath}"
            )
            
            # Открываем папку с файлом
            os.startfile(os.path.dirname(filepath))
            
        except Exception as e:
            messagebox.showerror("Ошибка", f"Ошибка при сохранении файла:\n{str(e)}")
            logger.error(f"Ошибка сохранения: {str(e)}")

    def download_report(self):
        try:
            # Получение и валидация базовых параметров
            token = self.token_entry.get().strip()
            counter_id = self.counter_entry.get().strip()
            login = self.login_entry.get().strip()
            date1 = self.date1_entry.get().strip()
            date2 = self.date2_entry.get().strip()

            # Проверка заполнения полей
            if not all([token, counter_id, date1, date2]):
                messagebox.showerror("Ошибка", "Заполните обязательные поля")
                return

            # Проверка метрик
            selected_metrics = self.get_selected_metrics()
            if not selected_metrics:
                messagebox.showerror("Ошибка", "Выберите хотя бы одну метрику")
                return

            # Формируем обязательные метрики
            required_metrics = ["ym:s:date", "ym:s:clientID"]
            for metric in required_metrics:
                if metric not in selected_metrics:
                    selected_metrics.insert(0, metric)

            # Подробное логирование
            logger.info("=== Подготовка запроса ===")
            logger.info(f"Даты: {date1} - {date2}")
            logger.info(f"Метрики: {', '.join(selected_metrics)}")
            logger.info(f"Counter ID: {counter_id}")

            # Получаем выбранный тип атрибуции
            attribution = self.attribution.get().split(' - ')[0]

            # Выполнение запроса с дополнительной проверкой
            try:
                data = fetch_report(
                    login=login,
                    token=token,
                    counter_id=counter_id,
                    report_type=self.report_type.get(),
                    metrics=selected_metrics,
                    date1=date1,
                    date2=date2,
                    attribution=attribution
                )

                # Вместо автоматического сохранения вызываем диалог
                self.save_report(data)

            except Exception as e:
                logger.error(f"Ошибка при получении отчета: {str(e)}")
                raise

        except Exception as e:
            messagebox.showerror("Ошибка", str(e))
            logger.error(f"Ошибка при выполнении запроса: {str(e)}")

    def load_settings(self):
        """Загрузка сохраненных установочных данных"""
        try:
            if os.path.exists(self.settings_file):
                with open(self.settings_file, 'r', encoding='utf-8') as f:
                    settings = json.load(f)
                    
                # Заполняем поля сохраненными значениями
                if 'token' in settings:
                    self.token_entry.delete(0, tk.END)
                    self.token_entry.insert(0, settings['token'])
                if 'counter_id' in settings:
                    self.counter_entry.delete(0, tk.END)
                    self.counter_entry.insert(0, settings['counter_id'])
                if 'login' in settings:
                    self.login_entry.delete(0, tk.END)
                    self.login_entry.insert(0, settings['login'])
                
                logger.info("Установочные данные успешно загружены")
        except Exception as e:
            logger.error(f"Ошибка загрузки установочных данных: {str(e)}")

    def save_settings(self):
        """Сохранение установочных данных"""
        try:
            settings = {
                'token': self.token_entry.get().strip(),
                'counter_id': self.counter_entry.get().strip(),
                'login': self.login_entry.get().strip()
            }
            
            with open(self.settings_file, 'w', encoding='utf-8') as f:
                json.dump(settings, f, ensure_ascii=False, indent=2)
                
            logger.info("Установочные данные успешно сохранены")
        except Exception as e:
            logger.error(f"Ошибка сохранения установочных данных: {str(e)}")

    def on_closing(self):
        """Обработчик закрытия окна"""
        self.save_settings()
        self.root.destroy()

def main():
    root = tk.Tk()
    app = MetrikaApp(root)
    root.mainloop()

if __name__ == "__main__":
    main()
