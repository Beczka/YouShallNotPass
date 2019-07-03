using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GeoCoordinatePortable;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Options;
using Newtonsoft.Json;

namespace MQTTClient
{
    class Program
    {
        static void Main(string[] args)
        {
            Connect().GetAwaiter().GetResult();
            Console.ReadLine();
        }

        private static DateTime ConvertTimeStampToDateTime(double unixTimeStamp)
        {
            DateTime dtDateTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc);
            dtDateTime = dtDateTime.AddMilliseconds(unixTimeStamp).ToLocalTime();
            return dtDateTime;
        }

        private static async Task Connect()
        {
            var factory = new MqttFactory();

            var options = new MqttClientOptionsBuilder()
                .WithTcpServer("127.0.0.1", 1883) // Port is optional
                .WithKeepAlivePeriod(TimeSpan.FromSeconds(10))
                .WithCommunicationTimeout(TimeSpan.FromSeconds(10))
                .Build();

            var mqttClient = factory.CreateMqttClient();

            mqttClient.UseConnectedHandler(async e =>
            {
                Console.WriteLine("### CONNECTED WITH SERVER ###");

                // Subscribe to a topic
                await mqttClient.SubscribeAsync(new TopicFilterBuilder().WithTopic("carCoordinates").Build());

                Console.WriteLine("### SUBSCRIBED ###");
            });

            var cars = new Dictionary<int, Car>
            {
                {0, new Car()},
                {1, new Car()},
                {2, new Car()},
                {3, new Car()},
                {4, new Car()},
                {5, new Car()}
            };

            var connection = mqttClient.ConnectAsync(options).GetAwaiter().GetResult();

            mqttClient.UseDisconnectedHandler(async e =>
            {
                Console.WriteLine("### DISCONNECTED FROM SERVER ###");
                await Task.Delay(TimeSpan.FromSeconds(5));

                try
                {
                    await mqttClient.ConnectAsync(options);
                }
                catch
                {
                    Console.WriteLine("### RECONNECTING FAILED ###");
                }
            });


            mqttClient.UseApplicationMessageReceivedHandler(e =>
            {
                var msg = Encoding.UTF8.GetString(e.ApplicationMessage.Payload);

                var item = JsonConvert.DeserializeObject<CarStatus>(msg);

                if (cars[item.carIndex].CarStatuses.Count != 0)
                {
                    var lastLocation = cars[item.carIndex].CarStatuses.Peek();

                    if (lastLocation != null)
                    {
                        var sCoord = new GeoCoordinate(lastLocation.location.lat, lastLocation.location.@long);
                        var eCoord = new GeoCoordinate(item.location.lat, item.location.@long);
                        var car = cars[item.carIndex];

                        var distance = sCoord.GetDistanceTo(eCoord);
                        cars[item.carIndex].DistanceInMeters += distance;

                        var kph = (distance / 1000.0f) / ((ConvertTimeStampToDateTime(item.timestamp) - ConvertTimeStampToDateTime(lastLocation.timestamp)).TotalSeconds / 3600.0f);
                        var mph = kph / 1.609f;

                        var payload = new CarStatus
                        {
                            timestamp = item.timestamp,
                            carIndex = item.carIndex,
                            type = "SPEED",
                            value = mph
                        };

                        var payload2 = new CarStatus
                        {
                            timestamp = item.timestamp,
                            carIndex = item.carIndex,
                            type = "POSITION",
                            value = cars.OrderByDescending(x => x.Value.DistanceInMeters).Select(x => x.Value).ToList().IndexOf(car) + 1
                        };


                        var json = JsonConvert.SerializeObject(payload);
                        var json2 = JsonConvert.SerializeObject(payload2);

                        var message = new MqttApplicationMessageBuilder()
                            .WithTopic("carStatus")
                            .WithPayload(json)
                            .WithExactlyOnceQoS()
                            .WithRetainFlag()
                            .Build();

                        var message2 = new MqttApplicationMessageBuilder()
                            .WithTopic("carStatus")
                            .WithPayload(json2)
                            .WithExactlyOnceQoS()
                            .WithRetainFlag()
                            .Build();

                        mqttClient.PublishAsync(message);
                        mqttClient.PublishAsync(message2);

                        //todo this throws TimeoutException exception
                        //var position = 1;
                        //var carPositions = cars.OrderByDescending(x => x.Value.DistanceInMeters).ToList();
                        //carPositions.ForEach(async element =>
                        //{
                        //    var payload2 = new CarStatus
                        //    {
                        //        timestamp = element.Value.CarStatuses.Peek().timestamp,
                        //        carIndex = element.Value.CarStatuses.Peek().carIndex,
                        //        type = "POSITION",
                        //        value = position
                        //    };

                        //    var json2 = JsonConvert.SerializeObject(payload2);

                        //    var message2 = new MqttApplicationMessageBuilder()
                        //        .WithTopic("carStatus")
                        //        .WithPayload(json2)
                        //        .WithExactlyOnceQoS()
                        //        .WithRetainFlag()
                        //        .Build();

                        //    await Task.Run(() => mqttClient.PublishAsync(message2));
                        //    position += 1;
                        //});
                    }
                }

                cars[item.carIndex].CarStatuses.Push(item);

            });
        }


        public class Car
        {
            public Stack<CarStatus> CarStatuses { get; set; }
            public int Position { get; set; }
            public double DistanceInMeters { get; set; }

            public Car()
            {
                CarStatuses = new Stack<CarStatus>();
            }
        }

        public class CarStatus
        {
            public double timestamp { get; set; }
            public int carIndex { get; set; }
            public string type { get; set; }
            public double value { get; set; }
            public Location location { get; set; }
        }

        public class Location
        {
            public double lat { get; set; }
            public double @long { get; set; }
        }
    }
}