# Задание 1

Итак, пришло время автоматизировать базовую отчетность нашего приложения. Давайте наладим автоматическую отправку аналитической сводки в телеграм каждое утро! 

Создайте своего телеграм-бота. 
Напишите скрипт для сборки отчета по ленте новостей. Отчет должен состоять из двух частей:

* текст с информацией о значениях ключевых метрик за предыдущий день
* график с значениями метрик за предыдущие 7 дней

Отобразите в отчете следующие ключевые метрики: 

* DAU 
* Просмотры
* Лайки
* CTR

Автоматизируйте отправку отчета с помощью Airflow. Отчет должен приходить ежедневно в 11:00 в чат. 

# Ответ

* [скрипт с запуском отправки отчета](https://github.com/usermarat/DA_simulator/blob/main/5.Reporting_automation/Task_7_1.py)
* отправляемый отчет:

![Report](https://github.com/usermarat/DA_simulator/assets/87779469/ca0c3490-299d-49ad-a7b8-2f0bb6b6ec16)

* схема получившегося DAG:

![DAG graph](https://github.com/usermarat/DA_simulator/assets/87779469/0d49d90b-12b1-4a58-acb5-e04a7d578e36)

* график его исполнения:

![DAG sched](https://github.com/usermarat/DA_simulator/assets/87779469/dc4a094a-5944-4125-a2b3-042cdab35627)

* [графики из отчета](https://github.com/usermarat/DA_simulator/tree/main/5.Reporting_automation/report_1)

# Задание 2

Соберите отчет по работе всего приложения как единого целого. 

Продумайте, какие метрики необходимо отобразить в этом отчете? Как можно показать их динамику? 
Приложите к отчету графики или файлы, чтобы сделать его более наглядным и информативным. 
Отчет должен быть не просто набором графиков или текста, а помогать отвечать бизнесу на вопросы о работе всего приложения совокупно. 
В отчете обязательно должны присутствовать метрики приложения как единого целого, или можно отобразить метрики по каждой из частей приложения — по ленте новостей и по мессенджеру.  

Автоматизируйте отправку отчета с помощью Airflow. 
Отчет должен приходить ежедневно в 11:00 в чат. 

# Ответ

* [скрипт с запуском отправки отчета](https://github.com/usermarat/DA_simulator/blob/main/5.Reporting_automation/Task_7_2.py)
* отправляемый отчет:

![Report 1](https://github.com/usermarat/DA_simulator/assets/87779469/fafbb866-9b1b-43ab-aae7-6d32a3222493)

![Report 2](https://github.com/usermarat/DA_simulator/assets/87779469/628e4df9-bc2f-4ccf-b100-e3c715e3605f)

![Report 3](https://github.com/usermarat/DA_simulator/assets/87779469/fc82db28-49cd-4f38-9fe2-b6abde32cbf9)

![Report 4](https://github.com/usermarat/DA_simulator/assets/87779469/379121ae-f6a2-41c4-95e4-c061dc25c7f5)

* схема получившегося DAG:

![DAG graph](https://github.com/usermarat/DA_simulator/assets/87779469/4cb2e142-d3c8-4c26-9f9a-5754bfe5ebd2)

* график его исполнения:

![DAG sched](https://github.com/usermarat/DA_simulator/assets/87779469/66e9ef63-2d08-483a-8e79-f06dc728a56d)

* [графики из отчета](https://github.com/usermarat/DA_simulator/tree/main/5.Reporting_automation/report_2)
