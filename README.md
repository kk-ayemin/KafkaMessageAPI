# Kafka Message API

## Description
<p>A simple ASP.net API using hosted consumer services.</p>
<p>The producer produces a random number for keys and a different random number for messages.</p>
<p>The hosted consumer then processes this message and produces an error when the message's random number exceeds 700.</p>
<p>This number is then added to the dead letter queue (DLQ) topic which is then saved to an excel file. The excel file is saved on a daily basis.</p>

## Tecnologies Used
<ul>
<li>Confluent.Kafka</li>
<li>Polly resilience API </li>
<li>Newtonsoft.Json JSON Parser</li>
<li>EPPlus to save excel files</li>
</ul>

## Installation
To install this project, first clone the git repo and build the project. Dependencies have already been added

```bash
git clone https://github.com/KafkaMessageAPI
```

<p>Then add this code to your appsettings.json file for the Kafka Broker configs. You don't have to change this (yet). </p>

```json
  "KafkaConfig": {
    "BootstrapServers": "192.168.2.131:9092",
    "SecurityProtocol": "Plaintext",
    "GroupId": "KafkaMessageApi",
    "AutoOffsetReset": "Earliest",
    "Topic": "sms.notifications",
    "EnableAutoCommit": false,
    "AutoCommitIntervalMs": 5000,
    "Acks": "Leader"
  }
```

<p>Then build the project as usual</p>

```bash
cd KafkaMessageAPI
dotnet build
```

### DISCLAIMER
<p>This is a something I wrote to showcase how hosted services and producers and consumers can be used with DLQ. It might not follow all of our coding standards so please help me fix those. </p>

<div style="text-align: center;">
    <img src="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTWDbV5rbr2JGtUjB_GqFJODgVbNrU1RNhD0Q&s" alt="Smile" width="25%">
</div>


