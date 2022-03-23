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

package types

import (
	"fmt"
	"github.com/tkeel-io/rule-util/pkg/errors"
)

var (
	ErrMetadataNotReachable = fmt.Errorf("metadata service not reachable")
	ErrCreatTaskFail        = fmt.Errorf("creat task fail")
	ErrDeployTaskFail       = fmt.Errorf("deploy task fail")
	ErrRecvUnknownType      = errors.New("recv error(unknown type)")

	ErrFilterFalse     = errors.New("rule eval filter false")
	ErrTaskUserIDEmpty = errors.New("task userID empty")
	ErrOptionParseFail = errors.New("parse option empty")

	ErrRestore = fmt.Errorf("metadata restore error")
	ErrSync    = fmt.Errorf("metadata sync error")

	ErrDecode = fmt.Errorf("message decode fail")
)
