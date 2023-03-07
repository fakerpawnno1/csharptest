using System.Text;
using Org.Apache.Rocketmq;

namespace rocketmq.Tests
{
    public class FifoMsgTest
    {
        [Fact]
        public async void TestSendFifoMsgSyncSimpleConsumerRecv()
        {
            TestInit.Init();
            List<string> sendMsgIdList = new List<string>();
            List<string> recvMsgIdList = new List<string>();
            int sendNum = 10;
            const string accessKey = "b6n13M5Ux7owi0eM";
            const string secretKey = "wQnW2rPn7eOodXxw";

            // Credential provider is optional for client configuration.
            var credentialsProvider = new StaticSessionCredentialsProvider(accessKey, secretKey);
            const string endpoints = "rmq-cn-c4d2ykf8r0o.cn-hangzhou.rmq.aliyuncs.com:8080";
            var clientConfig = new ClientConfig.Builder()
                .SetEndpoints(endpoints)
                .SetCredentialsProvider(credentialsProvider)
                .Build();

            const string topic = "testcsharpfifo";
            // In most case, you don't need to create too many producers, single pattern is recommended.
            // Producer here will be closed automatically.
            await using var producer = await new Producer.Builder()
                // Set the topic name(s), which is optional but recommended.
                // It makes producer could prefetch the topic route before message publishing.
                .SetTopics(topic)
                .SetClientConfig(clientConfig)
                .Build();
            const string tag = "tagA";
            const string consumerGroup = "GID_testcsharpfifo";
            var subscription = new Dictionary<string, FilterExpression>
                { { topic, new FilterExpression(tag) } };
            // In most case, you don't need to create too many consumers, single pattern is recommended.
            await using var simpleConsumer = await new SimpleConsumer.Builder()
                .SetClientConfig(clientConfig)
                .SetConsumerGroup(consumerGroup)
                .SetAwaitDuration(TimeSpan.FromSeconds(15))
                .SetSubscriptionExpression(subscription)
                .Build();

            for (int i = 0; i < sendNum; i++)
            {
                var bytes = Encoding.UTF8.GetBytes("foobar");
                const string messageGroup = "messageGroup1";
                var message = new Message.Builder()
                    .SetTopic(topic)
                    .SetBody(bytes)
                    .SetTag(tag)
                    // You could set multiple keys for the single message actually.
                    .SetKeys("yourMessageKey-7044358f98fc")
                    // Message group decides the message delivery order.
                    .SetMessageGroup(messageGroup)
                    .Build();


                var sendReceipt = await producer.Send(message);
                Console.WriteLine($"send fifo msg, message =" + message);
                Console.WriteLine(sendReceipt.MessageId);
                sendMsgIdList.Add(sendReceipt.MessageId);
            }

            var now = DateTime.UtcNow + TimeSpan.FromSeconds(30);
            while (true)
            {
                if (DateTime.Compare(DateTime.UtcNow, now) >= 1 || recvMsgIdList.Count >= sendNum)
                {
                    break;
                }

                var messageViews = await simpleConsumer.Receive(16, TimeSpan.FromSeconds(15));
                foreach (var message in messageViews)
                {
                    Console.WriteLine($"Ack fifo msg, message =" + message);
                    await simpleConsumer.Ack(message);
                    recvMsgIdList.Add(message.MessageId);
                }
                Thread.Sleep(2000);
            }
            await producer.DisposeAsync();
            await simpleConsumer.DisposeAsync();
            Assert.Equal(sendMsgIdList.Count, sendNum);
            Assert.Equal(recvMsgIdList.Count, sendNum);
            Assert.Equal(recvMsgIdList, sendMsgIdList);
        }
        

        [Fact]
        public async void TestSendFifoMsgSyncSimpleConsumerRecvSecond()
        {
            TestInit.Init();
            List<string> sendMsgIdList = new List<string>();
            List<string> recvMsgIdList = new List<string>();
            int sendNum = 10;
            const string accessKey = "b6n13M5Ux7owi0eM";
            const string secretKey = "wQnW2rPn7eOodXxw";

            // Credential provider is optional for client configuration.
            var credentialsProvider = new StaticSessionCredentialsProvider(accessKey, secretKey);
            const string endpoints = "rmq-cn-c4d2ykf8r0o.cn-hangzhou.rmq.aliyuncs.com:8080";
            var clientConfig = new ClientConfig.Builder()
                .SetEndpoints(endpoints)
                .SetCredentialsProvider(credentialsProvider)
                .Build();

            const string topic = "testcsharpfifo";
            // In most case, you don't need to create too many producers, single pattern is recommended.
            // Producer here will be closed automatically.
            await using var producer = await new Producer.Builder()
                // Set the topic name(s), which is optional but recommended.
                // It makes producer could prefetch the topic route before message publishing.
                .SetTopics(topic)
                .SetClientConfig(clientConfig)
                .Build();
            const string tag = "tagA";
            const string consumerGroup = "GID_testcsharpfifo";
            var subscription = new Dictionary<string, FilterExpression>
                { { topic, new FilterExpression(tag) } };
            // In most case, you don't need to create too many consumers, single pattern is recommended.
            await using var simpleConsumer = await new SimpleConsumer.Builder()
                .SetClientConfig(clientConfig)
                .SetConsumerGroup(consumerGroup)
                .SetAwaitDuration(TimeSpan.FromSeconds(15))
                .SetSubscriptionExpression(subscription)
                .Build();

            for (int i = 0; i < sendNum; i++)
            {
                var bytes = Encoding.UTF8.GetBytes("foobar");
                const string messageGroup = "messageGroup2";
                var message = new Message.Builder()
                    .SetTopic(topic)
                    .SetBody(bytes)
                    .SetTag(tag)
                    // You could set multiple keys for the single message actually.
                    .SetKeys("yourMessageKey-7044358f98fc")
                    // Message group decides the message delivery order.
                    .SetMessageGroup(messageGroup)
                    .Build();


                var sendReceipt = await producer.Send(message);
                Console.WriteLine($"send fifo msg, message =" + message);
                Console.WriteLine(sendReceipt.MessageId);
                sendMsgIdList.Add(sendReceipt.MessageId);
            }

            var now = DateTime.UtcNow + TimeSpan.FromSeconds(30);
            while (true)
            {
                if (DateTime.Compare(DateTime.UtcNow, now) >= 1 || recvMsgIdList.Count >= sendNum)
                {
                    break;
                }

                var messageViews = await simpleConsumer.Receive(16, TimeSpan.FromSeconds(15));
                foreach (var message in messageViews)
                {   
                    if (message.DeliveryAttempt > 1)
                    {
                        Console.WriteLine($"Ack fifo msg, message =" + message);
                        await simpleConsumer.Ack(message);
                        recvMsgIdList.Add(message.MessageId);
                    }
                    else
                    {
                        Console.WriteLine($"not ack fifo msg, message =" + message);
                    }
                }
                Thread.Sleep(2000);
            }
            await producer.DisposeAsync();
            await simpleConsumer.DisposeAsync();
            Assert.Equal(sendMsgIdList.Count, sendNum);
            Assert.Equal(recvMsgIdList.Count, sendNum);
            Assert.Equal(recvMsgIdList, sendMsgIdList);
        }
    }
}