
using System;
using System.Collections.Generic;
using System.Threading;


namespace rocketmq.Tests
{
    public static class TestInit
    {

        private volatile static object locker = new object();
        //接入点
        public static string MQ_NAMESERVER = "MQ_NAMESERVER";
        // AccessKey 阿里云身份验证，在阿里云服务器管理控制台创建
        public static string MQ_AK = "MQ_AK";
        // SecretKey 阿里云身份验证，在阿里云服务器管理控制台创建
        public static string MQ_SK = "MQ_SK";

        //SDK协议
        public static string MQ_PROTOCOL = "MQ_PROTOCOL";

        //SDK语言
        public static string MQ_CLIENT_LANGUAGE = "MQ_CLIENT_LANGUAGE";

        // SDK版本
        public static string MQ_CLIENT_VERSION = "MQ_CLIENT_VERSION";


        // 设置HTTP接入域名（此处以公共云生产环境为例）
        public static string _endpoint = null;
        // AccessKey 阿里云身份验证，在阿里云服务器管理控制台创建
        public static string _accessKeyId = null;
        // SecretKey 阿里云身份验证，在阿里云服务器管理控制台创建
        public static string _secretAccessKey = null;


        public static string _clientProtocol = null;
        public static string _clientLanguage = null;
        public static string _clientVersion = null;

        // 所属的 normal Topic
        public static string _normalTopicName = null;

        public static string _normalGroupId = null;

        // 所属的 dealy Topic
        public static string _delayTopicName = null;

        public static string _delayGroupId = null;

        public static string _transTopicName = null;

        public static string _transGroupId = null;

        public static string _orderTopicName = null;

        public static string _orderGroupId = null;

        public static string _msgTag = null;

        public volatile static bool inited = false;

        public static void Init()
        {
            lock (locker)
            {
                if (!inited)
                {

                    Console.WriteLine("======Init Start======");

                    _endpoint = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable(MQ_NAMESERVER)) ? Environment.GetEnvironmentVariable(MQ_NAMESERVER) : "rmq-cn-c4d2ykf8r0o.cn-hangzhou.rmq.aliyuncs.com:8080";

                    _accessKeyId = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable(MQ_AK)) ? Environment.GetEnvironmentVariable(MQ_AK) : "b6n13M5Ux7owi0eM";
                    // SecretKey 阿里云身份验证，在阿里云服务器管理控制台创建s
                    _secretAccessKey = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable(MQ_SK)) ? Environment.GetEnvironmentVariable(MQ_SK) : "wQnW2rPn7eOodXxw";

                    _clientProtocol = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable(MQ_PROTOCOL)) ? Environment.GetEnvironmentVariable(MQ_PROTOCOL) : "tcp";

                    _clientLanguage = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable(MQ_CLIENT_LANGUAGE)) ? Environment.GetEnvironmentVariable(MQ_CLIENT_LANGUAGE) : "csharp";

                    _clientVersion = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable(MQ_CLIENT_VERSION)) ? Environment.GetEnvironmentVariable(MQ_CLIENT_VERSION) : "0.0.3";

                    // 所属的 delay Topic
                    _normalTopicName = _clientLanguage + _clientProtocol + "_csttest_normal";

                    _normalGroupId = "GID_" + _clientLanguage + _clientProtocol + _clientVersion.Replace(".", "_") + "_normal";

                    // 所属的 dealy Topic
                    _delayTopicName = _clientLanguage + _clientProtocol + "_csttest_delay";

                    _delayGroupId = "GID_" + _clientLanguage + _clientProtocol + _clientVersion.Replace(".", "_") + "_delay";

                    // 所属的 dealy Topic
                    _transTopicName = _clientLanguage + _clientProtocol + "_csttest_trans";

                    _transGroupId = "GID_" + _clientLanguage + _clientProtocol + _clientVersion.Replace(".", "_") + "_trans";

                    // 所属的 dealy Topic
                    _orderTopicName = _clientLanguage + _clientProtocol + "_csttest_order";

                    _orderGroupId = "GID_" + _clientLanguage + _clientProtocol + _clientVersion.Replace(".", "_") + "_order";

                    _msgTag = _clientLanguage + "_" + _clientVersion;

                    inited = true;

                    Console.WriteLine(_normalTopicName);
                    Console.WriteLine("======Init End======");
                }
            }

        }

    }
}

