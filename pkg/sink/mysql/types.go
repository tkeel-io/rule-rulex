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

package mysql

import (
	"fmt"
	"github.com/tkeel-io/rule-rulex/internal/types"
	"github.com/tkeel-io/rule-rulex/pkg/sink"
	"github.com/tkeel-io/rule-rulex/pkg/sink/utils"
	"go.uber.org/atomic"
	"sync"
)

const (
	EntityType = "mysql"

	THING_PROPERTY_TYPE_INT32   = "int32"
	THING_PROPERTY_TYPE_FLOAT32 = "float"
	THING_PROPERTY_TYPE_FLOAT64 = "double"
	THING_PROPERTY_TYPE_ARRAY   = "array"
	THING_PROPERTY_TYPE_BOOL    = "bool"
	THING_PROPERTY_TYPE_ENUM    = "enum"
	THING_PROPERTY_TYPE_DATE    = "date"
	THING_PROPERTY_TYPE_STRUCT  = "struct"
	THING_PROPERTY_TYPE_STRING  = "string"
)

var (
	NewMessage        = utils.NewMessage
	Execute           = utils.Execute
	NewMessageContext = utils.NewMessageContext
)

func init() {
	fmt.Println("inti mysql")
	sink.Registered(EntityType, newAction)
}

func newAction(entityType, entityID string) types.Action {
	return newMysql(entityType, entityID)
}

func newMysql(entityType, entityID string) *mysql {
	return &mysql{
		entityType: entityType,
		entityID:   entityID,
		wg:         &sync.WaitGroup{},
		inited:     atomic.NewBool(false),
	}
}
