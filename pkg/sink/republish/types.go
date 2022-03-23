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

package republish

import (
	"fmt"
	"github.com/tkeel-io/rule-rulex/pkg/sink/utils"
)

var (
	ErrActionMetadataNil        = fmt.Errorf("action metadata is nil")
	ErrActionTypeNotFound       = fmt.Errorf("action type not found")
	ErrActionMetadataTopicEmpty = fmt.Errorf("action metadata topic empty")
	ErrRepublishTopicEmpty      = fmt.Errorf("republish topic empty")
	ErrRepublishNewTopicEmpty   = fmt.Errorf("republish new topic empty")
	ErrRepublishSendError       = fmt.Errorf("republish send error")
	ErrRepublishSinkStreamError = fmt.Errorf("republish Stream nil")
)
var (
	Execute = utils.Execute
)
