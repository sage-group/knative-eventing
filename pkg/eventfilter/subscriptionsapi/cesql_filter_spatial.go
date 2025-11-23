/*
Copyright 2022 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package subscriptionsapi

import (
	"fmt"

	cesql "github.com/cloudevents/sdk-go/sql/v2"
	cefn "github.com/cloudevents/sdk-go/sql/v2/function"
	"github.com/go-spatial/geom"
	"github.com/go-spatial/geom/encoding/wkt"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// Intersects creates a user-defined function that checks if two geometries intersect spatially.
// Both arguments should be WKT (Well-Known Text) formatted geometry strings.
// Examples: "POINT(1 1)", "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))"
// Note: This implementation uses bounding box intersection for performance.
var Intersects cesql.Function = cefn.NewFunction(
	"INTERSECTS",
	[]cesql.Type{cesql.StringType, cesql.StringType},
	nil,
	cesql.BooleanType,
	func(event cloudevents.Event, i []interface{}) (interface{}, error) {
		geom1Str := i[0].(string)
		geom2Str := i[1].(string)

		// Parse the first geometry from WKT
		geom1, err := wkt.DecodeString(geom1Str)
		if err != nil {
			return false, fmt.Errorf("failed to parse first geometry: %w", err)
		}

		// Parse the second geometry from WKT
		geom2, err := wkt.DecodeString(geom2Str)
		if err != nil {
			return false, fmt.Errorf("failed to parse second geometry: %w", err)
		}

		// Get bounding boxes for intersection check
		extent1, err := geom.NewExtentFromGeometry(geom1)
		if err != nil {
			return false, fmt.Errorf("failed to get extent for first geometry: %w", err)
		}

		extent2, err := geom.NewExtentFromGeometry(geom2)
		if err != nil {
			return false, fmt.Errorf("failed to get extent for second geometry: %w", err)
		}

		// Check if bounding boxes intersect
		_, intersects := extent1.Intersect(extent2)
		return intersects, nil
	},
)
