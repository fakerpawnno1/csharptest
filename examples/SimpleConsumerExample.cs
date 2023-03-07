/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NLog;
using Org.Apache.Rocketmq;

namespace examples
{
    internal static class SimpleConsumerExample
    {
        private static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();

        internal static async Task QuickStart()
        {
            const string accessKey = "b6n13M5Ux7owi0eM";
            const string secretKey = "wQnW2rPn7eOodXxw";

            // Credential provider is optional for client configuration.
            var credentialsProvider = new StaticSessionCredentialsProvider(accessKey, secretKey);
            const string endpoints = "rmq-cn-c4d2ykf8r0o.cn-hangzhou.rmq.aliyuncs.com:8080";
            var clientConfig = new ClientConfig.Builder()
                .SetEndpoints(endpoints)
                .SetCredentialsProvider(credentialsProvider)
                .Build();

            // Add your subscriptions.
            const string consumerGroup = "GID_testcsharptrans";
            const string topic = "testcsharptrans";
            var subscription = new Dictionary<string, FilterExpression>
                { { topic, new FilterExpression("*") } };
            // In most case, you don't need to create too many consumers, single pattern is recommended.
            await using var simpleConsumer = await new SimpleConsumer.Builder()
                .SetClientConfig(clientConfig)
                .SetConsumerGroup(consumerGroup)
                .SetAwaitDuration(TimeSpan.FromSeconds(15))
                .SetSubscriptionExpression(subscription)
                .Build();
            while (true)
            {
                var messageViews = await simpleConsumer.Receive(16, TimeSpan.FromSeconds(15));
                foreach (var message in messageViews)
                {   Console.WriteLine($"Received a message, message= "+ message);
                    Logger.Info($"Received a message, topic={message.Topic}, message-id={message.MessageId}");
                    await simpleConsumer.Ack(message);
                    Console.WriteLine($"Message is acknowledged successfully, message= "+ message);
                    Logger.Info($"Message is acknowledged successfully, message-id={message.MessageId}");
                    // await simpleConsumer.ChangeInvisibleDuration(message, TimeSpan.FromSeconds(15));
                    // Logger.Info($"Changing message invisible duration successfully, message=id={message.MessageId}");
                }
            }
           
        }
    }
}