package postgresql

const (
	POSTGRESQL_DB               = `CREATE DATABASE IF NOT EXISTS iot_manage`
	POSTGRESQL_IOT_THING_CREATE = `CREATE TABLE iot_manage.device_thing_data (
				time 					UInt64,
				user_id 				String,
				device_id 				String,
				source_id 				String,
				thing_id   				String,
				identifier				String,
				value_int32				Int32,
				value_float				Float32,
				value_double			Float64,
				value_string			String,
				value_enum  			Enum8('0'=0,'1'=1,'2'=2,'3'=3,'4'=4,'5'=5,'6'=6,'7'=7,'8'=8,'9'=9,'10'=10,'11'=11,'12'=12,'13'=13,'14'=14,'15'=15,'16'=16,'17'=17,'18'=18,'19'=19,'20'=20),
				value_bool				UInt8,
				value_string_ex 		String,
				value_array_string 		Array(String),
				value_array_int32 		Array(Int32),
				value_array_float	 	Array(Float32),
				value_array_double 		Array(Float64),
				action_date				Date,
				action_time 			DateTime
			) Engine=ReplacingMergeTree() PARTITION BY toYYYYMM(action_date) ORDER BY (time,user_id,thing_id,identifier,device_id,intHash64(time)) TTL action_date + INTERVAL 10 DAY SAMPLE BY intHash64(time) SETTINGS index_granularity=8192`
MYSQL_IOT_DEVICE_EVENT_DATA = `CREATE TABLE iot_manage.device_event_data (
				device_id 		String,
				source_id 		String,
				event_id   	    String,
				identifier		String,
				type			String,
				user_id	  		String,
				thing_id 		String,
				metadata		String,
				time			UInt64,
				action_date		Date,
				action_time 	DateTime
			) Engine=ReplacingMergeTree() PARTITION BY toYYYYMM(action_date) ORDER BY (time,user_id,device_id,event_id)  TTL action_date + INTERVAL 10 DAY SETTINGS index_granularity=8192`
)
