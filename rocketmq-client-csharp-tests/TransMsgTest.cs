using System.Collections.Concurrent;
using System.Text;
using NLog;
using Org.Apache.Rocketmq;

namespace rocketmq.Tests
{
    public class TransMsgTest
    {   
        private static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();
        private class TransactionChecker : ITransactionChecker
        {
            private volatile ConcurrentBag<string> _sendMsgIds;
            private readonly  bool _isCommit;
            public TransactionChecker(ref ConcurrentBag<string>  sendMsgIds,bool isCommit)
            {
                _sendMsgIds = sendMsgIds;
                _isCommit = isCommit;
            }

            public TransactionResolution Check(MessageView messageView)
            {
                if (Int32.Parse(messageView.Properties["TRANSACTION_CHECK_TIMES"]) > 1)
                {   
                    Logger.Info($"Checker rollback trans msg, messageview={messageView}");
                    Console.WriteLine($"Checker rollback trans msg, messageview={messageView}");
                    return TransactionResolution.ROLLBACK;
                }
                _sendMsgIds.Add(messageView.MessageId);
                if (_isCommit)
                {   
                    Logger.Info($"Checker commit trans msg, messageview={messageView}");
                    Console.WriteLine($"Checker commit trans msg, messageview={messageView}");
                    return TransactionResolution.COMMIT;
                }
                Logger.Info($"Checker rollback trans msg, messageview={messageView}");
                Console.WriteLine($"Checker rollback trans msg, messageview={messageView}");
                return TransactionResolution.ROLLBACK;
            }
        }

        [Fact]
        public async void TestSendTransHalfCommitMsgSyncSimpleConsumerRecv()
        {
            TestInit.Init();
            ConcurrentBag<string> sendMsgIdList = new ConcurrentBag<string>();
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

            const string topic = "testcsharptrans1";
            // In most case, you don't need to create too many producers, single pattern is recommended.
            // Producer here will be closed automatically.
            await using var producer = await new Producer.Builder()
                // Set the topic name(s), which is optional but recommended.
                // It makes producer could prefetch the topic route before message publishing.
                .SetTopics(topic)
                .SetClientConfig(clientConfig)
                .SetTransactionChecker(new TransactionChecker(ref sendMsgIdList,true))
                .Build();

            const string consumerGroup = "GID_testcsharptrans";
            const string tag = "halfcommitbycheck";
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
                var transaction = producer.BeginTransaction();
                // Define your message body.
                var bytes = Encoding.UTF8.GetBytes("foobar");
                var message = new Message.Builder()
                    .SetTopic(topic)
                    .SetBody(bytes)
                    .SetTag(tag)
                    // You could set multiple keys for the single message actually.
                    .SetKeys("keyA","keyB")
                    .Build();

                var sendReceipt = await producer.Send(message, transaction);
                Console.WriteLine($"Send trans msg, sendReceipt={sendReceipt}");
                // Commit the transaction.
                if (i % 2 == 0)
                {
                    transaction.Commit();
                    sendMsgIdList.Add(sendReceipt.MessageId);
                    Console.WriteLine($"Commit trans msg, sendReceipt={sendReceipt}");
                    Logger.Info($"Commit trans msg, sendReceipt={sendReceipt}");
                }
            }

            var now = DateTime.UtcNow + TimeSpan.FromSeconds(120);
            while (true)
            {
                var messageViews = await simpleConsumer.Receive(16, TimeSpan.FromSeconds(15));
                foreach (var message in messageViews)
                {
                    Console.WriteLine($"Ack trans msg, message =" + message);
                    await simpleConsumer.Ack(message);
                    recvMsgIdList.Add(message.MessageId);
                }
                if (DateTime.Compare(DateTime.UtcNow, now) >= 0 || (recvMsgIdList.Count >= sendNum && sendMsgIdList.Count==sendNum))
                {
                    break;
                }
                Thread.Sleep(3000);
            }
            var sendMsgIds=sendMsgIdList.ToList();
            sendMsgIds.Sort();
            recvMsgIdList.Sort();
            Assert.Equal(sendMsgIdList.Count, sendNum);
            Assert.Equal(recvMsgIdList.Count, sendNum);
            Assert.Equal(recvMsgIdList, sendMsgIds);
            await producer.DisposeAsync();
            await simpleConsumer.DisposeAsync();
        }
        
          [Fact]
        public async void TestSendTransCheckRollbackMsgSyncSimpleConsumerRecv()
        {
            TestInit.Init();
            ConcurrentBag<string> sendMsgIdList = new ConcurrentBag<string>();
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

            const string topic = "testcsharptrans1";
            // In most case, you don't need to create too many producers, single pattern is recommended.
            // Producer here will be closed automatically.
            await using var producer = await new Producer.Builder()
                // Set the topic name(s), which is optional but recommended.
                // It makes producer could prefetch the topic route before message publishing.
                .SetTopics(topic)
                .SetClientConfig(clientConfig)
                .SetTransactionChecker(new TransactionChecker(ref sendMsgIdList,false))
                .Build();

            const string consumerGroup = "GID_testcsharptrans";
            const string tag = "halfrollbackbychecker";
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
                var transaction = producer.BeginTransaction();
                // Define your message body.
                var bytes = Encoding.UTF8.GetBytes("foobar");
                var message = new Message.Builder()
                    .SetTopic(topic)
                    .SetBody(bytes)
                    .SetTag(tag)
                    // You could set multiple keys for the single message actually.
                    .SetKeys("keyA","keyB")
                    .Build();

                var sendReceipt = await producer.Send(message, transaction);
                Console.WriteLine($"Send trans msg, sendReceipt={sendReceipt}");
                // Commit the transaction.
                if (i % 2 == 0)
                {
                    transaction.Rollback();
                    sendMsgIdList.Add(sendReceipt.MessageId);
                    Logger.Info($"Rollback trans msg, sendReceipt={sendReceipt}");
                    Console.WriteLine($"Rollback trans msg, sendReceipt={sendReceipt}");
                }
            }

            var now = DateTime.UtcNow + TimeSpan.FromSeconds(120);
            while (true)
            {
                var messageViews = await simpleConsumer.Receive(16, TimeSpan.FromSeconds(15));
                foreach (var message in messageViews)
                {  
                    await simpleConsumer.Ack(message);
                    Console.WriteLine($"Ack trans msg, message =" + message);
                    recvMsgIdList.Add(message.MessageId);
                }
                if (DateTime.Compare(DateTime.UtcNow, now) >= 0 || sendMsgIdList.Count==sendNum)
                {
                    break;
                }
                Thread.Sleep(3000);
            }
            Assert.Equal(sendMsgIdList.Count, sendNum);
            Assert.Equal(recvMsgIdList.Count, 0);
            await producer.DisposeAsync();
            await simpleConsumer.DisposeAsync();
        }
        
        
            [Fact]
        public async void TestSendTransCheckRollbackMsgSyncSimpleConsumerRecvRetrys()
        {
            TestInit.Init();
            ConcurrentBag<string> sendMsgIdList = new ConcurrentBag<string>();
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

            const string topic = "testcsharptrans1";
            // In most case, you don't need to create too many producers, single pattern is recommended.
            // Producer here will be closed automatically.
            await using var producer = await new Producer.Builder()
                // Set the topic name(s), which is optional but recommended.
                // It makes producer could prefetch the topic route before message publishing.
                .SetTopics(topic)
                .SetClientConfig(clientConfig)
                .SetTransactionChecker(new TransactionChecker(ref sendMsgIdList,false))
                .Build();

            const string consumerGroup = "GID_testcsharptrans";
            const string tag = "testtransretry";
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
                var transaction = producer.BeginTransaction();
                // Define your message body.
                var bytes = Encoding.UTF8.GetBytes("foobar");
                var message = new Message.Builder()
                    .SetTopic(topic)
                    .SetBody(bytes)
                    .SetTag(tag)
                    // You could set multiple keys for the single message actually.
                    .SetKeys("keyA","keyB")
                    .Build();

                var sendReceipt = await producer.Send(message, transaction);
                // Commit the transaction.
                transaction.Commit();
                sendMsgIdList.Add(sendReceipt.MessageId);
                Console.WriteLine($"Commit trans msg, sendReceipt={sendReceipt}");
                Logger.Info($"Commit trans msg, sendReceipt={sendReceipt}");
                
            }

            var now = DateTime.UtcNow + TimeSpan.FromSeconds(120);
            while (true)
            {
                var messageViews = await simpleConsumer.Receive(16, TimeSpan.FromSeconds(15));
                foreach (var message in messageViews)
                {  
                    if (message.DeliveryAttempt > 2)
                     {
                         Console.WriteLine($"Ack trans msg, message =" + message);
                         await simpleConsumer.Ack(message);
                         recvMsgIdList.Add(message.MessageId);
                     }
                     else
                     {
                         Console.WriteLine($"not ack trans msg, message =" + message);
                     }
                }
                if (DateTime.Compare(DateTime.UtcNow, now) >= 0 || recvMsgIdList.Count==sendNum)
                {
                    break;
                }
                Thread.Sleep(3000);
            }
            
            var sendMsgIds=sendMsgIdList.ToList();
            sendMsgIds.Sort();
            recvMsgIdList.Sort();
            Assert.Equal(sendMsgIdList.Count, sendNum);
            Assert.Equal(recvMsgIdList.Count, sendNum);
            Assert.Equal(recvMsgIdList, sendMsgIds);
            await producer.DisposeAsync();
            await simpleConsumer.DisposeAsync();
        }
        
    }
}