using System;
using System.Configuration;
using System.Collections;
using System.Collections.Generic;
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
			return collection;
		}

		private MongoDatabase GetDatabase()
		{
			if(string.IsNullOrWhiteSpace(ConnectionString))
                throw new ConfigurationErrorsException("MongoDbAppender.ConnectionString is required!");
			
			MongoUrl url = MongoUrl.Create(ConnectionString);
            MongoServerSettings settings = MongoServerSettings.FromUrl(url);
            var conn = new MongoServer(settings);

            string dbname = string.IsNullOrWhiteSpace(url.DatabaseName) ? "log4net" : url.DatabaseName;
            MongoDatabase db = conn.GetDatabase(dbname);
			return db;
		}

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