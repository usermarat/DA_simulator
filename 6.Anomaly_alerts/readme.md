# Задание

Напишите систему алертов для нашего приложения.

Система должна с периодичность каждые 15 минут проверять ключевые метрики — такие как активные пользователи в ленте / мессенджере, просмотры, лайки, CTR, количество отправленных сообщений. 

Изучите поведение метрик и подберите наиболее подходящий метод для детектирования аномалий. 

В случае обнаружения аномального значения, в чат должен отправиться алерт — сообщение со следующей информацией: метрика, ее значение, величина отклонения.

В сообщение можно добавить дополнительную информацию, которая поможет при исследовании причин возникновения аномалии. Это может быть, например, график, ссылки на дашборд/чарт в BI системе. 

# Ответ

* [скрипт с автозапуском проверок](https://github.com/usermarat/DA_simulator/blob/main/6.Anomaly_alerts/Task_8.py)
* график исполнения DAG:

![DAG graph](https://github.com/usermarat/DA_simulator/assets/87779469/259a909c-f311-4dd8-a455-bedaeb4385d9)

* примеры отработки алертов:

![dau feed](https://github.com/usermarat/DA_simulator/assets/87779469/1fd18bf0-1ab8-43d5-bfe6-7c15bcb8e984)

![likes](https://github.com/usermarat/DA_simulator/assets/87779469/19ab554b-e0eb-4831-a7d5-c039520f64fb)

![ctr](https://github.com/usermarat/DA_simulator/assets/87779469/a2d70f7f-833e-4dad-918c-72cea064213a)

![dau](https://github.com/usermarat/DA_simulator/assets/87779469/a4b8ece6-96e8-4320-ad61-351400ee2688)

![mess](https://github.com/usermarat/DA_simulator/assets/87779469/939fb286-b69a-4e6c-950c-79f0789c15b3)

* [дашборд контролируемых показателей](https://github.com/usermarat/DA_simulator/blob/main/6.Anomaly_alerts/alert-dashboard.jpg)
