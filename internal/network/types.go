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

package conn

import "math"

//ERROR_VERSION error version
const MAX_VERSION = int64(math.MaxInt64)

//MIN_VERSION
const MIN_VERSION = int64(math.MinInt64)

//ERROR_VERSION
const ERROR_VERSION = int64(-1)


// resourceVersionGetter is an interface used to get resource version from events.
// We can't reuse an interface from meta otherwise it would be a cyclic dependency and we need just this one method
type resourceVersionGetter interface {
	GetResourceVersion() string
}
