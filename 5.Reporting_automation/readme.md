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

![Report](https://github.com/usermarat/DA_simulator/assets/87779469/5a813cc5-b50c-4213-aaab-0c489234483a)

* схема получившегося DAG:

![DAG graph](https://github.com/usermarat/DA_simulator/assets/87779469/590b253e-1962-4e99-bfcc-b2efb317153b)

* график его исполнения:

![DAG sched](https://github.com/usermarat/DA_simulator/assets/87779469/1bda7917-4429-4c52-ba6b-d771de745599)

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

![Report 1](https://github.com/usermarat/DA_simulator/assets/87779469/0ad69db0-3bf5-4130-ac83-63021029dfcb)

![Report 2](https://github.com/usermarat/DA_simulator/assets/87779469/c1438947-615e-4bf7-9f47-5b26dbfd14c4)

![Report 3](https://github.com/usermarat/DA_simulator/assets/87779469/f89f429c-33cb-4129-8a77-4a742cc4d2d6)

![Report 4](https://github.com/usermarat/DA_simulator/assets/87779469/79a95f2e-f7f1-42fe-8c53-cb65cd23b8f2)

* схема получившегося DAG:

![DAG graph](https://github.com/usermarat/DA_simulator/assets/87779469/bb3a0534-7495-4586-ad15-cc43a669b223)

* график его исполнения:

![DAG sched](https://github.com/usermarat/DA_simulator/assets/87779469/68ece0e7-2180-4cab-bb07-f0201e56b89d)

* [графики из отчета](https://github.com/usermarat/DA_simulator/tree/main/5.Reporting_automation/report_2)
