# Partners of the Payment System
Проектная работа курса [Data Engineer](https://otus.ru/lessons/data-engineer/) от Otus.

Проект основан на реальной системе. Для предотвращения раскрытия коммерческой информации название сервиса и компаний-партнёров изменены.

## Задание
Сервис платёжной системы для своих партнёров.

Платёжная система предоставляет своим партнёрам программы привелегий. Партнеры предоставляют привелегии держателям карт. Партнёры загружают данные о предоставленных привилегиях держателям карт. Передача информации о привилегиях осуществляется путём загрузки в систему реестров, представляющих собой текстовые файлы в формате CSV. Партнёры загружают данные в своих форматах. Загруженные данные преобразуются к единому формату.

Отчётным периодом в системе является календарный месяц. Приём информации от партнёров по отчётному периоду длится в течение периода сбора, который начинается в первый день месяца, следующего за отчётным периодом, и длится в течение 15 дней. По истечении периода сбора расчётный период закрывается и блокируется, становясь недоступным для внесения в него каких-либо изменений. По итогам закрытия расчётного периода осуществляется формирование отчётности.

Список партнёров может меняться. Исходный список партнёров:
- Cinema
- Retail
- Taxi
- Telecom

## Архитектура
...

## Реализация
Система реализована на сервисах [Google Cloud Platform](https://cloud.google.com):
- Файлы с данными загружаются в [Cloud Storage](https://cloud.google.com/storage/)
- Для DWH используется [BigQuery](https://cloud.google.com/bigquery/)
- В качестве планировщика используется [Cloud Scheduler](https://cloud.google.com/scheduler/)
- Для передачи сообщений используется [Cloud Pub/Sub](https://cloud.google.com/pubsub/)
- Скрипты [Cloud Functions](https://cloud.google.com/functions/) срабатывают по событиям
- Для построения отчётов используется [Data Studio](datastudio.google.com)

### DWH
Для моделирование DWH используется метод [Data Vault](https://en.wikipedia.org/wiki/Data_vault_modeling).

Исходный код (DDL) таблиц и процедур находится в папке *DWH*

В DWH хранятся справочники, данные партнёров и отчёты.

К справочникам относятся:
- BINS - первые 6 цифр банковских карт
- PRIVILEGES - перечень привелегий, предоставляемых партнёрами клиентам
- CITY - справочник корректных названий городов и стран, в которых они находятся

#### Stage
Справочники загружаются в область *dicts*. Партнёры загружают реестры в виде файлов в область *inbox* в папку со своим именем.

Справочники могут быть сразу созданы в детальном слое. Но так как они могут измениться, они подаются в систему в виде файлов и проходят все этапы обработки.

Входящие файлы без изменений попадают в область Stage:
- STG_BINS - справочник Bins
- STG_PRIVILEGES - справочник привелегий
- STG_CITY - справочник городов

Реестры (данные, загружаемые партнёрами) загружаются по окончании периода сбора данных (16-го числа следующего месяца).
- STG_CINEMA - данные партнёра Cinema
- STG_RETAIL - данные партнёра Retail
- STG_TAXI - данные партнёра Taxi
- STG_TELECOM - данные партнёра Telecom

#### ODS
Справочники очищаются и перегружаются в область ODS сразу после загрузки в область Stage:
- ODS_BINS - справочник Bins
- ODS_PRIVILEGES - справочник привелегий
- ODS_CITY - справочник городов

Реестры (данные, загружаемые партнёрами) проверяются и очищаются. Перегружаются только корректные записи отчётного периода:
- ODS_CINEMA - данные партнёра Cinema
- ODS_RETAIL - данные партнёра Retail
- ODS_TAXI - данные партнёра Taxi
- ODS_TELECOM - данные партнёра Telecom

#### DDS
Данные из области ODS приводятся к единому формату и перегружаются в область DDS:
- Партнёры:
  - HUB_PARTNERS
  - SAT_PARTNERS
- Данные:
  - HUB_DATA
  - SAT_DATA
  - LNK_DATA_PARTNERS
  - LNK_DATA_BINS
  - LNK_DATA_PRIVILEGES
  - LNK_DATA_CITY
- Справочник Bins:
  - HUB_BINS
  - SAT_BINS
- Справочник привелегий
  - HUB_PRIVILEGES
  - SAT_PRIVILEGES
- Справочник городов
  - HUB_CITY
  - SAT_CITY

#### Data Marts
По данным из DDS строятся отчёты в области Data Marts:
- DM_LOADS - отчёты о загрузках данных
- DM_REPORT - отчёты рассчётных периодов
- V_DM_CARD - отчёт по типам карт
- V_DM_BANK - отчёт по банкам
- V_DM_CITY - отчёт по городам
- V_DM_PRIVILEGE - отчёт по типам привилегий
- V_DM_PAYMENT - отчёт по среднему чеку

#### Процедуры обработки данных
Для заполнения данными таблиц в DWH созданы следующие процедуры:
- ADD_PARTNER() - добавление нового партнёра
- UPDATE_PARTNER() - обновление партнёра (можно поменять тэг)
- DELETE_PARTNER() - удаление партнёра (партнёр становится неактивным)
- LOAD_BINS() - загрузка данных о Bins в слой DDS
- LOAD_PRIVILEGE() - загрузка данных о привилегиях в слой DDS
- LOAD_CINEMA() - загрузка данных партнёра Cinema во временные таблицы
- LOAD_RETAIL() - загрузка данных партнёра Retail во временные таблицы
- LOAD_TAXI() - загрузка данных партнёра Taxi во временные таблицы
- LOAD_TELECOM() - загрузка данных партнёра Telecom во временные таблицы
- LOAD_DATA() - перегрузка данных из временных таблиц в слой DDS
- DATA2DM() - заполнение витрины слоя DM отчётом за конкретный период

Таблицы и процедуры создаются в наборе данных *PP*.

#### Таблицы для партнёров
При добавлении в систему нового партнёра для него должны быть созданы таблицы в областях Stage и ODS, процедура перегрузки данных в область DDS и представления (View) в области DM с витринами, содержащие данные только этого партнёра.

Код создания таблиц и процедур генерируется по шаблону, описывающему поля реестров (данных партнёров) и их преобразований к единому формату.

Шаблоны партнёров находятся в папке папке *DWH/Partners/templates*

Исходный код генератора кода находится в папке *src/ddl-gen*

Скрипт **genall.sh** генерирует код для всех партнёра.

### Загрузка данных
При загрузки файлов со справочниками в Storage срабатывает функция **LoadDicts**. Файлы со справочниками должны называться так же как называются таблицы справочников, и должны содержать весь справочник.

Для обработки файлов с реестрами по расписанию (16-го числа каждого месяца в 00:00) планировщик Scheduler через систему Pub/Sub посылает сообщение, которое запускает функцию **LoadData**.

Обе функции:
- загружают данные из файлов в область Stage
- вызывают процедуру перегрузки данных из области Stage в области ODS и DDS
- удаляют файлы

Исторические данные (реестры за прошлые периоды) в Cloud Storage не выгружаются. Для их загрузки используется скрипт **load_hist_data**. Он загружает данные из файлов в область Stage, вызывает процедуру перегрузки данных в области ODS и DDS, а затем вызывает процедуру заполнения витрины DM отчётом за предыдущие периоды. Файлы с данными не удаляются.

Исходный код функций загрузки данных находится в папке *src/data-load*

## Данные

Сгененированные данные и справочники находятся в папке *data*

### Реестры
Для предотвращения раскрытия коммерческой информации используются сгенерированные синтетические данные. Данные генерируются специальным скриптом **data-gen** по шаблонам, описывающих поля и их значения.

Вызов: **data-gen** <-t | --template> filename
                <-o | --output> filename") 
                <-y | --year> number") 
                <-m | --month> number") 
                [<-n | --number-lines> number]") 
                [<-e | --errors> number]") 
                [<-v | --verbose>]") 
                [<-h | --help>]") 
 
 где:
  - template - файл с шаблоном
  - output - файл со сгенерированными данными
  - year - год, для генерации дат
  - month - месяц, для генерации дат
  - number-lines - кол-во генерируемых записей, по умолчанию - 100000
  - errors - процент записей с ошибками, по умолчанию - 0
  - verbose - флаг вывода информации о работе
  - help - вывод этой информации

Скрипт **gen2019.sh** генерирует данные за весь 2019 год для всех шаблонов с 100000 записей с 10% ошибок в каждом месяце для каждого партнёра.

Исходный код генератора данных находится в папке *src/data-gen*

### Справочники
В системе предусмотрено наличие справочников:
- Bins - справочник Bins (первые 6 цифр карт)
- Privileges - справочник привелегий
- City - справочник корректных названий городов и стран, в которых они находятся

В справочниках коммерческие данные анонимизированы.

## Отчёты
По витринам слоя DM построены отчёты в Data Studio:
- [Загрузки](https://datastudio.google.com/reporting/0647ce66-575e-425f-b565-9bd6f15be617)
- [Регионы](https://datastudio.google.com/reporting/0ad44459-d2a3-479b-82a1-e2c68d530653)
- [Банки](https://datastudio.google.com/reporting/572e7340-116f-43d3-a726-403ac57c1c2d)
- [Карты](https://datastudio.google.com/reporting/12c1d2ae-fc93-45f3-ada7-742a51e27da1)
- [Города](https://datastudio.google.com/reporting/8ed4a56d-b71d-4d50-8509-c2293b9db467)
- [Страны](https://datastudio.google.com/reporting/ca54084c-bc3b-4c6d-a674-5ab7b5d1bf34)
- [Привелегии](https://datastudio.google.com/reporting/85b47b60-36c5-465a-96aa-154e2c9be091)
- [Средний чек](https://datastudio.google.com/reporting/6f6e728e-a195-4b9b-b80b-596a728c4f66)

## Развитие
Для полноты реализации в систему планируется добавить пользовательское приложение в виде web-сервиса для загрузки данных и просмотров отчётов.

## Перспектива
В дальнейшем к системе можно будет добавить обработку потоковых данных о транзакциях партнёров.
