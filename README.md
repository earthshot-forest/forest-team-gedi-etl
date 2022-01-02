# forest-team-gedi-etl
Pull, clip, and store GEDI level 1b, 2a, 2b, and 4b data.

# In progress:
- Turn this into a command line tool for public use
- Integrate Rabbit MQ, so the downloads and process steps happen asynchronously.
- Explore adding an API layer

## To install your own RabbitMQ server and set it up
Use this link for info on installing the server and starting it. https://www.rabbitmq.com/download.html

### In CLI for Ubuntu, here are some useful commands:
| CLI Command Names     | CLI Command                               |
| --------------------- | ----------------------------------------- |
| Start RabbitMQ server | `$ sudo systemctl start rabbitmq-server`  |
| Stop local node       | `$ sudo systemctl stop rabbitmq-server`   |
| View status of server | `$ sudo systemctl status rabbitmq-server` |
| Quit out of status    | `q`                                       |

If using windows, use CLI commands from here: https://www.rabbitmq.com/rabbitmq-service.8.html

Before testing using RabbitMQ, remember to start the server. When you are done, remember to stop the server.
