using System;
using System.Configuration;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Linq;

using MongoDB.Bson;
using MongoDB.Driver;
using log4net.Appender;
using log4net.Core;

namespace Log4Mongo
{
	public class MongoDBAppender : AppenderSkeleton
	{
		private readonly List<MongoAppenderFileld> _fields = new List<MongoAppenderFileld>();

		/// <summary>
		/// MongoDB database connection in the format:
		/// mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]
		/// See http://www.mongodb.org/display/DOCS/Connections
		/// If no database specified, default to "log4net"
		/// </summary>
		public string ConnectionString { get; set; }

		/// <summary>
		/// Name of the collection in database
		/// Defaults to "logs"
		/// </summary>
		public string CollectionName { get; set; }

        public void AddField(MongoAppenderFileld fileld)
		{
			_fields.Add(fileld);
		}

		protected override void Append(LoggingEvent loggingEvent)
		{
			var collection = GetCollection();
			collection.Insert(BuildBsonDocument(loggingEvent));
		}

		protected override void Append(LoggingEvent[] loggingEvents)
		{
			var collection = GetCollection();
			collection.InsertBatch(loggingEvents.Select(BuildBsonDocument));
		}

		private MongoCollection GetCollection()
		{
			MongoDatabase db = GetDatabase();

            string colName = string.IsNullOrWhiteSpace(CollectionName) ? "logs" : CollectionName;
            MongoCollection collection = db.GetCollection(colName);

            if (Interlocked.CompareExchange(ref _collectionSharded, 1, 0) == 0)
            {
                if (!string.IsNullOrWhiteSpace(ShardKey))
                    ShardCollection(collection);
            }
			return collection;
		}

        static int _dbSharded = 0, _collectionSharded = 0;

        const string DEFAULT_DB = "log4net";
        const string DEFAULT_CONNECTION = "mongodb://localhost/";
		private MongoDatabase GetDatabase()
		{
            Uri u = new Uri(string.IsNullOrWhiteSpace(ConnectionString) ? DEFAULT_CONNECTION + DEFAULT_DB : ConnectionString);
			MongoUrl url = MongoUrl.Create(u.ToString());
            MongoServerSettings settings = MongoServerSettings.FromUrl(url);
            var conn = new MongoServer(settings);

            string dbname = string.IsNullOrWhiteSpace(url.DatabaseName) ? DEFAULT_DB : url.DatabaseName;
            MongoDatabase db = conn.GetDatabase(dbname);

            if (Interlocked.CompareExchange(ref _dbSharded, 1, 0) == 0)
            {
                if (!string.IsNullOrWhiteSpace(ShardKey))
                    EnableSharding(db);
            }
            return db;
		}

        #region sharding option

        /// <summary>
        /// If set, will enable sharding on database and shard the collection base on this shard key
        /// </summary>
        /// <remarks>If cluster is not sharded and option is set, appender will throw!</remarks>
        public string ShardKey { get; set; }

        void EnableSharding(MongoDatabase db)
        {
            var cmd = new CommandDocument("enablesharding", db.Name);
            try
            {
                var dbCfg = new MongoDatabaseSettings(db.Server, "admin");
                var adminDb = new MongoDatabase(db.Server, dbCfg);
                db.Server.Connect();
                adminDb.RunCommand(cmd);
            }
            catch (MongoCommandException mex)
            {
                if (!mex.Message.ToLower().Contains("already enabled"))
                    throw;
                else
                    Console.WriteLine("Sharding already enabled!");
            }
        }

        void ShardCollection(MongoCollection col)
        {
            var cmd = new CommandDocument
            {
                { "shardCollection", col.Database.Name + '.' + col.Name },
                { "key", new BsonDocument(this.ShardKey, 1) },
            };
            try
            {
                MongoServer server = col.Database.Server;
                var dbCfg = new MongoDatabaseSettings(server, "admin");
                var adminDb = new MongoDatabase(server, dbCfg);
                server.Connect();
                CommandResult shcr = adminDb.RunCommand(cmd);
            }
            catch (MongoCommandException mex)
            {
                if (!mex.Message.ToLower().Contains("already "))
                    throw;
                else
                    Console.WriteLine("Sharding already enabled!");
            }
        }

        #endregion

        private BsonDocument BuildBsonDocument(LoggingEvent log)
		{
			if(_fields.Count == 0)
			{
				return BackwardCompatibility.BuildBsonDocument(log);
			}
			var doc = new BsonDocument();
			foreach(MongoAppenderFileld field in _fields)
			{
				object value = field.Layout.Format(log);
				BsonValue bsonValue = BsonValue.Create(value);
				doc.Add(field.Name, bsonValue);
			}
			return doc;
		}
	}
}