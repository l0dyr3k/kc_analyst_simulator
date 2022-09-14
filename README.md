# kc-analyst-simulator

Привет! В этом репозитории находятся решения всех задач с курса "Симулятор аналитика" от [KARPOV.COURSES](https://karpov.courses)

Сам курс подразумевал введение в работу начинающего аналитика в стартапе, где необходимо выстроить аналитические процессы, связанные с работой приложения. Приложение включало в себя ленту новостей и мессенжер

<details>
  
<summary> <kbd>А поэтапно вся работа выглядела так:</kbd> </summary>
  
* Построение дашбордов
  + Задание 1: основные метрики ленты новостей
  + Задание 2: взаимодействие ленты новостей и мессенджера
* Анализ продуктовых метрик
  + Задание 1: retention пользователей с разным трафиком
  + Задание 2: аномалия новых пользователей
  + Задание 3: просадок активных пользователей
  + Задание 4: поведение пользователей и недельный retention
* [A/B-тестирование](https://github.com/l0dyr3k/kc-analyst-simulator/tree/main/abtest)
  + [Задание 1: A/A-тестирование](https://github.com/l0dyr3k/kc-analyst-simulator/blob/main/abtest/abtest-1.ipynb)
  + [Задание 2: методы проверок гипотез](https://github.com/l0dyr3k/kc-analyst-simulator/blob/main/abtest/abtest-2.ipynb)
  + [Задание 3: линеаризованные метрики](https://github.com/l0dyr3k/kc-analyst-simulator/blob/main/abtest/abtest-3.ipynb)
* [ETL конвейер](https://github.com/l0dyr3k/kc-analyst-simulator/tree/main/etl-pipeline)
  + [Задание 1: построение базового ETL конвейера](https://github.com/l0dyr3k/kc-analyst-simulator/blob/main/etl-pipeline/etl-pipeline.py)
* [Автоматизация отчётов](https://github.com/l0dyr3k/kc-analyst-simulator/tree/main/report)
  + [Задание 1: основные метрики за прошедшую неделю](https://github.com/l0dyr3k/kc-analyst-simulator/blob/main/report/report-1.py)
  + [Задание 2: динамика работы приложения](https://github.com/l0dyr3k/kc-analyst-simulator/blob/main/report/report-2.py)
* [Система алёртов](https://github.com/l0dyr3k/kc-analyst-simulator/tree/main/alert)
  + [Задание 1: поиск аномалий в реальном времени](https://github.com/l0dyr3k/kc-analyst-simulator/tree/main/alert/alert.py)
  
</details>

----------------

### Стей технологий

Основные инструменты, использовавшиеся для работы:

* язык программирования Python и его библиотеки:
  + аналитические библиотеки [Pandas](https://pandas.pydata.org/), [Statsmodel](https://www.statsmodels.org/stable/index.html)
  + библиотеки для выполнения научных и инженерных расчётов [NumPy](https://numpy.org/), [SciPy](https://scipy.org/)
  + библиотеки для визуализации данных [Matplotlib](https://matplotlib.org/), [seaborn](https://seaborn.pydata.org/)
* cистема контроля версий проекта [GitLab](https://about.gitlab.com)
* среда программирования [Jupyter Notebook](https://jupyter.org/)
* язык запросов SQL [ClickHouse](https://clickhouse.com/docs/ru/)
* BI-системы и сервисы визуализации даннных [Superset](https://superset.apache.org/), [Redash](https://redash.io)
  
