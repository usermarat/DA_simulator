# Задание

Напишите систему алертов для нашего приложения.

Система должна с периодичность каждые 15 минут проверять ключевые метрики — такие как активные пользователи в ленте / мессенджере, просмотры, лайки, CTR, количество отправленных сообщений. 

Изучите поведение метрик и подберите наиболее подходящий метод для детектирования аномалий. 

В случае обнаружения аномального значения, в чат должен отправиться алерт — сообщение со следующей информацией: метрика, ее значение, величина отклонения.

В сообщение можно добавить дополнительную информацию, которая поможет при исследовании причин возникновения аномалии. Это может быть, например, график, ссылки на дашборд/чарт в BI системе. 

# Ответ

* [скрипт с автозапуском проверок](https://github.com/usermarat/DA_simulator/blob/main/6.Anomaly_alerts/Task_8.py)
* график исполнения DAG:

![DAG graph](https://github.com/usermarat/DA_simulator/assets/87779469/abb1debc-e296-4095-b2e2-f98fe9f5c58f)

* примеры отработки алертов:

![dau feed](https://github.com/usermarat/DA_simulator/assets/87779469/af8e98a6-92bf-4fd2-aa7d-8166c6e9dcb3)

![likes](https://github.com/usermarat/DA_simulator/assets/87779469/92d1c447-4b1c-4332-b4bf-c484193bafe0)

![ctr](https://github.com/usermarat/DA_simulator/assets/87779469/6d98126c-3bb6-4857-a74d-34b6f0ef504c)

![dau](https://github.com/usermarat/DA_simulator/assets/87779469/dd5fb740-4e36-4d74-94e0-1e273ed2d66f)

![mess](https://github.com/usermarat/DA_simulator/assets/87779469/c8049587-3076-465c-a8db-cc211fd22817)

* [дашборд контролируемых показателей](https://github.com/usermarat/DA_simulator/blob/main/6.Anomaly_alerts/alert-dashboard.jpg)
