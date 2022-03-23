/*
 * Copyright (C) 2019 Yunify, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this work except in compliance with the License.
 * You may obtain a copy of the License in the LICENSE file, or at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package rulex

import (
	"github.com/tkeel-io/rule-rulex/pkg/sink/bucket"
	"github.com/tkeel-io/rule-rulex/pkg/sink/chronus"
	"github.com/tkeel-io/rule-rulex/pkg/sink/kafka"
	"github.com/tkeel-io/rule-rulex/pkg/sink/mysql"
	"github.com/tkeel-io/rule-rulex/pkg/sink/postgresql"
	"github.com/tkeel-io/rule-rulex/pkg/sink/redis"
	"github.com/tkeel-io/rule-rulex/pkg/sink/republish"


)

var (
	TypRepublish = republish.EntityType
	TypKafka     = kafka.EntityType
	TypChronus   = chronus.EntityType
	TypBucket    = bucket.EntityType
	TypMysql = mysql.EntityType
	TypPostgresql = postgresql.EntityType
	TypRedis = redis.EntityType
)
