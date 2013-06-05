using System;
using System.Collections;
using System.Text;
using MongoDB.Bson;
using MongoDB.Driver;
using log4net.Core;
using log4net.Util;

namespace Log4Mongo
{
	public class BackwardCompatibility
	{
		public static BsonDocument BuildBsonDocument(LoggingEvent loggingEvent)
		{
			if(loggingEvent == null)
			{
				return null;
			}

			var toReturn = new BsonDocument {
				{"timestamp", loggingEvent.TimeStamp}, 
				{"level", loggingEvent.Level.ToString()}, 
				{"thread", loggingEvent.ThreadName ?? string.Empty}, 
				{"userName", loggingEvent.UserName}, 
				{"message", loggingEvent.RenderedMessage}, 
				{"loggerName", loggingEvent.LoggerName ?? string.Empty}, 
				{"domain", loggingEvent.Domain ?? string.Empty}, 
				{"machineName", Environment.MachineName ?? string.Empty}
			};

			// location information, if available
			if(loggingEvent.LocationInformation != null)
			{
				toReturn.Add("fileName", loggingEvent.LocationInformation.FileName);
				toReturn.Add("method", loggingEvent.LocationInformation.MethodName ?? string.Empty);
				toReturn.Add("lineNumber", loggingEvent.LocationInformation.LineNumber);
				toReturn.Add("className", loggingEvent.LocationInformation.ClassName ?? string.Empty);
			}

			// exception information
			if(loggingEvent.ExceptionObject != null)
			{
				toReturn.Add("exception", BuildExceptionBsonDocument(loggingEvent.ExceptionObject));
			}

			// properties
			PropertiesDictionary compositeProperties = loggingEvent.GetProperties();
			if(compositeProperties != null && compositeProperties.Count > 0)
			{
				var properties = new BsonDocument();
				foreach(DictionaryEntry entry in compositeProperties)
				{
					properties.Add(entry.Key.ToString(), entry.Value.ToString());
				}

				toReturn.Add("properties", properties);
			}

			return toReturn;
		}

		private static BsonDocument BuildExceptionBsonDocument(Exception ex)
		{
			var toReturn = new BsonDocument {
				{"message", ex.Message ?? string.Empty}, 
				{"source", ex.Source}, 
				{"stackTrace", ex.StackTrace}
			};

			if(ex.InnerException != null)
			{
				toReturn.Add("innerException", BuildExceptionBsonDocument(ex.InnerException));
			}

			return toReturn;
		}
	}
}