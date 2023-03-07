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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using Org.Apache.Rocketmq;

namespace examples
{
    internal static class ProducerTransactionMessageExample
    {
        private static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();

        private class TransactionChecker : ITransactionChecker
        {
            public TransactionResolution Check(MessageView messageView)
            {
                Console.WriteLine("Receive transaction check, messageView= "+ messageView);
                Logger.Info("Receive transaction check, messageId={}", messageView.MessageId);
                return TransactionResolution.ROLLBACK;
            }
        }

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

            const string topic = "testcsharptrans";
            // In most case, you don't need to create too many producers, single pattern is recommended.
            // Producer here will be closed automatically.
            await using var producer = await new Producer.Builder()
                // Set the topic name(s), which is optional but recommended.
                // It makes producer could prefetch the topic route before message publishing.
                .SetTopics(topic)
                .SetClientConfig(clientConfig)
                .SetTransactionChecker(new TransactionChecker())
                .Build();
           
            for (int i = 0; i < 50; i++)
            {    
                var transaction = producer.BeginTransaction();
                // Define your message body.
                var bytes = Encoding.UTF8.GetBytes("foobar");
                const string tag = "yourMessageTagA";
                var message = new Message.Builder()
                    .SetTopic(topic)
                    .SetBody(bytes)
                    .SetTag(tag)
                    // You could set multiple keys for the single message actually.
                    .SetKeys("yourMessageKey-7044358f98fc")
                    .Build();
                var sendReceipt = await producer.Send(message, transaction);
                if (i % 2 == 0)
                {
                    Console.WriteLine("Send transaction message , messageId= " + sendReceipt.MessageId);
                    Logger.Info("Send transaction message successfully, messageId={}", sendReceipt.MessageId);
                    // Commit the transaction.
                    transaction.Commit();
                    // Or rollback the transaction.
                    // transaction.Rollback();
                    // Or you could close the producer manually.
                    // await producer.DisposeAsync();
                }
            }
              Thread.Sleep(360000);
        }
    }
}