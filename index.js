const express = require("express");
const amqp = require("amqplib/callback_api");
const app = express();
const port = 8000;

let channel = null;
amqp.connect("amqp://localhost", (err, connection) => {
  if (err) {
    throw err;
  }

  connection.createChannel((err, ch) => {
    if (err) {
      throw err;
    }

    channel = ch;
    const TOPIC = "codingtest";
    channel.assertQueue(TOPIC);
  });
});

app.get("/event", (req, res) => {
  res.send(app.get("data"));
});

app.post("/publish/:topic", (req, res) => {
  const { topic } = req.params;
  try {
    channel.assertQueue(topic);
    channel.sendToQueue(topic, Buffer.from('{ message: "hello" }'));
    res.send({ topic, data: { message: "ok" } });
    setTimeout(function () {
      connection.close();
      process.exit(0);
    }, 500);
  } catch (error) {
    res.status(401).send(error.message);
  }
});

app.post("/subscribe/:topic", (req, res) => {
  const { params, body } = req;

  try {
    channel.assertQueue(params.topic);
    channel.consume(
      params.topic,
      (message) => {
        app.set("data", message.content.toString());
        res.send(message.content.toString());
        res.redirect(body.url);
      },
      { noAck: true }
    );
  } catch (error) {
    res.status(401).send(error.message);
  }
});

app.listen(port, () => {
  console.log(`Example app listening at http://localhost:${port}`);
});
