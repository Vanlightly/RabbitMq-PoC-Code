using Newtonsoft.Json.Linq;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQTestExamples.IntegrationTests.Helpers
{
    public class ConnectionKiller
    {
        private static HttpClient HttpClient;

        public static void Initialize(string nodeUrl, string username, string password)
        {
            if (HttpClient == null)
            {
                var authValue = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}")));

                HttpClient = new HttpClient();
                HttpClient.BaseAddress = new Uri($"http://{nodeUrl}");
                HttpClient.DefaultRequestHeaders.Authorization = authValue;
            }
        }

        public static async Task<List<string>> GetConnectionNamesAsync(int timeoutMs)
        {
            string responseBody = "[]";
            int counter = 0;
            while (responseBody.Equals("[]") && counter < timeoutMs)
            {
                var response = await HttpClient.GetAsync("api/connections");
                // keep trying until there are connections
                if (response.IsSuccessStatusCode)
                {
                    responseBody = await response.Content.ReadAsStringAsync();
                }

                if (responseBody.Equals("[]"))
                    await Task.Delay(100);

                counter+=100;
            }

            if (responseBody.Equals("[]"))
                return new List<string>();

            var json = JArray.Parse(responseBody);
            var conn = json.First();

            var connectionNames = new List<string>();
            while (conn != null)
            {
                var name = conn["name"];
                connectionNames.Add(name.Value<string>());
                conn = conn.Next;
            }

            return connectionNames;
        }
        
        public static async Task ForceCloseConnectionsAsync(List<string> connectionNames)
        {
            foreach (var name in connectionNames)
                await DeleteAsync($"api/connections/{name}");
        }

        private static async Task DeleteAsync(string path)
        {
            var response = await HttpClient.DeleteAsync(path);
            if (response.StatusCode != HttpStatusCode.NoContent)
            {
                throw new Exception(response.StatusCode.ToString());
            }
        }
    }
}
