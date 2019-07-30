package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/pmezard/gogeos/geos"
)

type Point struct {
	Lon int64 `json:"lon"`
	Lat int64 `json:"lat"`
}

type NodePoint struct {
	Id    int64
	Point Point
}

type NodePoints []NodePoint

func (points NodePoints) FindPoint(id int64) (NodePoint, error) {
	i := sort.Search(len(points), func(i int) bool {
		return points[i].Id >= id
	})
	if i == len(points) {
		return NodePoint{}, fmt.Errorf("cannot resolve node: %d", id)
	}
	return points[i], nil
}

func buildNodeArray(r *O5MReader) (NodePoints, error) {
	// Count nodes
	resets := []ResetPoint{}
	count := 0
	for r.Next() {
		if r.Kind() == ResetKind {
			resets = append(resets, r.ResetPoint())
			if len(resets) > 1 {
				break
			}
		} else if r.Kind() == NodeKind {
			if len(resets) == 0 {
				return nil, fmt.Errorf("node found before first reset")
			}
			count += 1
		}
	}
	if r.Err() != nil {
		return nil, r.Err()
	}
	if len(resets) != 2 {
		return nil, fmt.Errorf("more or less than 2 resets until nodes end")
	}

	// Collect nodes
	points := make([]NodePoint, count)
	err := r.Seek(resets[0])
	if err != nil {
		return nil, err
	}
	i := 0
	for r.Next() {
		if r.Kind() != NodeKind {
			continue
		}
		n := r.Node()
		points[i] = NodePoint{
			Id: n.Id,
			Point: Point{
				Lon: n.Lon,
				Lat: n.Lat,
			},
		}
		if i > 0 && points[i-1].Id >= points[i].Id {
			return nil, fmt.Errorf("nodes are not sorted by id: %d >= %d",
				points[i-1].Id, points[i].Id)
		}
		i += 1
		if i == len(points) {
			break
		}
	}
	if r.Err() != nil {
		return nil, r.Err()
	}
	if i != len(points) {
		return nil, fmt.Errorf("could not collect all nodes")
	}
	return NodePoints(points), r.Seek(resets[1])
}

var (
	IgnoredRingRoles = map[string]bool{
		// Apparently usde to delimit the city hall as an area or enclosing
		// linear, ignore it. Ex: Pinos Genil(346486)[level=8].
		"admin_centre": true,
	}
)

func buildGeometry(rings []*Linestring) ([]*geos.Geometry, error) {
	// Bail out on non-ring inputs
	for _, ring := range rings {
		if ring.Role == "inner" || ring.Role == "outer" || ring.Role == "" {
			continue
		} else {
			if _, ok := IgnoredRingRoles[ring.Role]; ok {
				continue
			}
			return nil, fmt.Errorf("unsupported ring role: %s", ring.Role)
		}
	}
	all, err := makeRings(rings)
	if err != nil {
		return nil, err
	}
	return makePolygons(all)
}

type Location struct {
	Type        string          `json:"type"`
	Coordinates [][][][]float64 `json:"coordinates"`
}

func linearRingToJson(r *geos.Geometry) ([][]float64, error) {
	typ, err := r.Type()
	if typ != geos.LINEARRING {
		return nil, err
	}
	pointCount, err := r.NPoint()
	if err != nil {
		return nil, err
	}
	if pointCount <= 0 {
		return nil, fmt.Errorf("empty linear ring")
	}
	coords, err := r.Coords()
	if err != nil {
		return nil, fmt.Errorf("cannot get coordinates: %s", err)
	}
	ring := make([][]float64, len(coords))
	for j, p := range coords {
		ring[j] = []float64{p.X, p.Y}
	}
	return ring, nil
}

func isClockwise(ring [][]float64) bool {
	if len(ring) < 3 {
		// Undefined
		return false
	}
	area := 0.
	for i := 1; i < len(ring); i++ {
		p1 := ring[i-1]
		p2 := ring[i]
		area += (p2[0] - p1[0]) * (p2[1] + p1[1])
	}
	p1 := ring[len(ring)-1]
	p2 := ring[0]
	area += (p2[0] - p1[0]) * (p2[1] + p1[1])
	return area > 0
}

func reverseJsonRing(ring [][]float64) {
	for i := 0; i < len(ring)/2; i++ {
		j := len(ring) - 1 - i
		ring[i], ring[j] = ring[j], ring[i]
	}
}

func polygonsToJson(polygons []*geos.Geometry) (*Location, error) {
	loc := &Location{
		Type: "multipolygon",
	}
	shapes := [][][][]float64{}
	for _, g := range polygons {
		typ, err := g.Type()
		if err != nil {
			return nil, err
		}
		if typ != geos.POLYGON {
			return nil, fmt.Errorf("cannot handle geometry type: %d", typ)
		}
		geomCount, err := g.NGeometry()
		if err != nil {
			return nil, err
		}
		if geomCount <= 0 {
			return nil, fmt.Errorf("empty geometry")
		}
		shell, err := g.Shell()
		if err != nil {
			return nil, err
		}
		holes, err := g.Holes()
		if err != nil {
			return nil, err
		}
		rings := make([][][]float64, 0, len(holes)+1)
		inner, err := linearRingToJson(shell)
		if err != nil {
			return nil, fmt.Errorf("cannot extract inner ring: %s", err)
		}
		if isClockwise(inner) {
			reverseJsonRing(inner)
		}
		rings = append(rings, inner)
		for _, hole := range holes {
			outer, err := linearRingToJson(hole)
			if err != nil {
				return nil, fmt.Errorf("cannot extract outer ring: %s", err)
			}
			if !isClockwise(outer) {
				reverseJsonRing(outer)
			}
			rings = append(rings, outer)
		}
		shapes = append(shapes, rings)
	}
	loc.Coordinates = shapes
	return loc, nil
}

type RelationJson struct {
	Id          string `json:"id"`
	Name        string `json:"name"`
	AdminLevel  int    `json:"admin_level,omitempty"`
	CountryIso2 string `json:"country_iso2,omitempty"`
	CountryIso3 string `json:"country_iso3,omitempty"`
	Center      struct {
		Lon float64 `json:"lon"`
		Lat float64 `json:"lat"`
	} `json:"center"`
	Location Location     `json:"shape"`
	Tags     []StringPair `json:"tags"`
}

type RelationTags struct {
	tags map[string]string
}

func NewRelationTags(rel *Relation) (*RelationTags, error) {
	tags := patchTags(rel)
	dict := map[string]string{}
	for _, tag := range tags {
		if _, ok := dict[tag.Key]; ok {
			return nil, fmt.Errorf("duplicate tag: %s=%s", tag.Key, tag.Value)
		}
		dict[tag.Key] = tag.Value
	}
	return &RelationTags{
		tags: dict,
	}, nil
}

func (rt *RelationTags) Name() string {
	name := rt.tags["name"]
	pos := strings.Index(name, "(")
	if pos >= 0 {
		// "France (terres)"
		name = name[:pos]
	}
	name = strings.TrimSpace(name)
	return name
}

func (rt *RelationTags) CountryIso2() string {
	iso2 := rt.tags["ISO3166-1"]
	if iso2 != "" {
		return iso2
	}
	return rt.tags["ISO3166-1:alpha2"]
}

func (rt *RelationTags) CountryIso3() string {
	return rt.tags["ISO3166-1:alpha3"]
}

func (rt *RelationTags) Tag(key string) string {
	return rt.tags[key]
}

func (rt *RelationTags) AdminLevel() (int, string) {
	v, ok := rt.tags["admin_level"]
	if !ok {
		return -1, v
	}
	level, err := strconv.ParseUint(v, 10, 32)
	if err != nil {
		return -1, v
	}
	return int(level), v
}

func (rt *RelationTags) PlaceType() string {
	return rt.tags["place"]
}

func makeJsonRelation(rel *Relation, center *Centroid, loc *Location) (
	*RelationJson, error) {

	if center == nil {
		return nil, fmt.Errorf("no center")
	}
	if loc == nil || len(loc.Coordinates) <= 0 {
		return nil, fmt.Errorf("empty relation")
	}
	r := &RelationJson{
		Id:       strconv.Itoa(int(rel.Id)),
		Location: *loc,
	}
	r.Center.Lon = center.Lon
	r.Center.Lat = center.Lat

	tags, err := NewRelationTags(rel)
	if err != nil {
		return nil, err
	}
	r.Name = tags.Name()
	level, levelStr := tags.AdminLevel()
	if level < 1 || level > 11 {
		placeType := tags.PlaceType()
		if placeType != "city" && placeType != "town" {
			return nil, fmt.Errorf("unexpected admin_level: %s", levelStr)
		}
	} else {
		r.AdminLevel = level
	}
	r.CountryIso2 = tags.CountryIso2()
	r.CountryIso3 = tags.CountryIso3()
	r.Tags = append(r.Tags, rel.Tags...)
	return r, nil
}

type sortedRefs []Ref

func (s sortedRefs) Len() int {
	return len(s)
}

func (s sortedRefs) Less(i, j int) bool {
	return s[i].Id < s[j].Id
}

func (s sortedRefs) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

var (
	IgnoredRelations = map[string]bool{
		"":                true, // at least on France/Spain shared territory
		"subarea":         true, // related but takes no part in geometry
		"subarea:FIXME":   true,
		"collection":      true, // deprecated
		"disused:subarea": true,
		"admin_centre":    true, // Node representing the administrative centre
		"label":           true, // Node representing where to draw the label
	}
)

func collectWayRefs(rel *Relation) ([]Ref, []Ref, error) {
	wayIds := []Ref{}
	relIds := []Ref{}
	for _, ref := range rel.Refs {
		if ref.Type != 1 {
			if ref.Type == 0 {
				// Points
				continue
			} else if ref.Type == 2 {
				// Relation
				if ref.Role == "outer" || ref.Role == "inner" {
					// Relations made of other relations (France borders)
					relIds = append(relIds, ref)
					continue
				}
				if IgnoredRelations[ref.Role] {
					continue
				}
				return nil, nil, fmt.Errorf("cannot handle relation relation: %s", ref.Role)
			} else {
				return nil, nil, fmt.Errorf("unsupported reference type: %d", ref.Type)
			}
		}
		wayIds = append(wayIds, ref)
	}
	sort.Sort(sortedRefs(wayIds))
	sort.Sort(sortedRefs(relIds))
	return wayIds, relIds, nil
}

func collectWayGeometries(wayIds []Ref, db *WaysDb) ([]*Linestring, error) {
	rings := []*Linestring{}
	if len(wayIds) <= 0 {
		return rings, nil
	}
	for _, ref := range wayIds {
		ring, err := db.Get(ref.Id)
		if err != nil {
			return nil, err
		}
		if ring == nil {
			return nil, fmt.Errorf("cannot resolve way: %d", ref.Id)
		}
		// People insist on typing "Outer" instead of "outer".
		ring.Role = strings.ToLower(ref.Role)
		rings = append(rings, ring)
	}
	return rings, nil
}

func collectRelationWays(relIds []Ref, db *WaysDb) ([]*Linestring, error) {
	rings := []*Linestring{}
	if len(relIds) <= 0 {
		return rings, nil
	}
	for _, ref := range relIds {
		rel, err := db.GetRelation(ref.Id)
		if err != nil {
			return nil, err
		}
		if rel == nil {
			return nil, fmt.Errorf("cannot resolve subrelation: %d", ref.Id)
		}
		wayIds, subIds, err := collectWayRefs(rel)
		if err != nil {
			return nil, err
		}
		if len(subIds) > 0 {
			lines, err := collectRelationWays(subIds, db)
			if err != nil {
				return nil, err
			}
			rings = append(rings, lines...)
		}
		ways, err := collectWayGeometries(wayIds, db)
		if err != nil {
			return nil, err
		}
		rings = append(rings, ways...)
	}
	return rings, nil
}

func getTag(rel *Relation, key string) string {
	for _, tag := range rel.Tags {
		if tag.Key == key {
			return tag.Value
		}
	}
	return ""
}

func isMultilineString(rel *Relation) bool {
	return getTag(rel, "type") == "multilinestring"
}

func patchRings(rel *Relation, rings []*Linestring) []*Linestring {
	if rel.Id != 1362232 {
		return rings
	}
	// Metropolitan France polygon is not closed
	rings = append(rings,
		&Linestring{
			Id: 0,
			Points: []Point{
				{-17641958, 433431448},
				{-17668244, 433425557},
			},
		},
		&Linestring{
			Id: 1,
			Points: []Point{
				{37501395, 434237009},
				{37469067, 434193643},
			},
		})
	return rings
}

func buildSpecialRelations(rel *Relation, db *WaysDb) ([]*geos.Geometry, error) {
	if rel.Id != 11980 {
		return nil, nil
	}
	// France (11980)
	// The main France relation is build from subrelations with "subarea" role.
	// Usually subareas are ignored but in this case we want to build the
	// geometry from them.
	geoms := []*geos.Geometry{}
	for _, ref := range rel.Refs {
		if ref.Type != 2 || ref.Role != "subarea" {
			continue
		}
		sub, err := db.GetRelation(ref.Id)
		if err != nil {
			return nil, fmt.Errorf("could not get subrelation %d: %s", ref.Id, err)
		}
		if sub == nil {
			// Ignore missing relations to handle non-planet files
			continue
		}
		fmt.Printf("Processing subrelation %s(%d)\n", sub.Name(), sub.Id)
		parts, err := buildRelationPolygons(sub, db)
		if err != nil {
			return nil, fmt.Errorf("cannot build subrelation %s(%d): %s",
				sub.Name(), sub.Id, err)
		}
		geoms = append(geoms, parts...)
	}
	return geoms, nil
}

func isRecursiveRelation(rel *Relation) bool {
	// In general, geometries are only built from the ways contained by the
	// relation. For historical reasons there seems to be a few exceptions,
	// where we have to extract the ways recursively from inner and outer
	// sub-relations.
	return rel.Id == 1111111 || // Germany
		rel.Id == 1362232 // France metropolitaine
}

func buildRelationPolygons(rel *Relation, db *WaysDb) ([]*geos.Geometry, error) {
	// Collect way and relation ids and sort them
	wayIds, relIds, err := collectWayRefs(rel)
	if err != nil {
		return nil, err
	}
	rings, err := collectWayGeometries(wayIds, db)
	if err != nil {
		return nil, err
	}
	if isRecursiveRelation(rel) {
		subRings, err := collectRelationWays(relIds, db)
		if err != nil {
			return nil, err
		}
		rings = append(rings, subRings...)
	}
	rings = patchRings(rel, rings)
	return buildGeometry(rings)
}

var (
	_ACCEPTED_BOUNDARIES = []string{
		// ACCEPTED
		"administrative",
		"administative",
		"admniistrative",
		"adminsitrative",
		"land_area",
		"landuse",
		"cdp",
		"postal_code",
		"territorial",
		"suburb",
		"borough",
		"neighbourhood",
		"political",
		"maritime",
		"adminstrative",
		"admininstrative",
		"adm",
		"civil",
		"region",
		"area",
		"local_authority",
		"public",
		"civil_parish",
		"city",
		"civic",
		"quarter",
		// This one is debatable and appears along with place=town. Ex: Chatham (445115).
		"place",
		// Comes with place=town. Ex: Melgaço (5756321).
		"urban",
		// West Yorkshire(88079)[level=6]
		"ceremonial",
	}
	_REJECTED_BOUNDARIES = []string{
		// REJECTED
		// Ignore French speaking part of Wallonie, subdivisions of Catalonia
		// comarques which are split on provinces, and disputed areas in Cyprus.
		"administrative_fraction",
		// Parks
		"national_park",
		"park",
		"state_park",
		"protected_area",
		"forestry",
		// Ignore things like Province apostolique de Normandie (2713391)
		"religious_administration",
		"religioius_administration",
		"religious_adminsitration",
		"religious",
		"religous_administration",
		"rreligious_administration",
		// Statistical/polling divisions
		"statistical",
		"census",
		// Historical
		"historical_administrative",
		"old_administrative",
		"obsolete_administrative",
		"obsolete_boundary",
		"historic:administrative",
		"historic",
		"historical",
		// Disputed
		"disputed",
		"claim",
		"aboriginal_lands",
		// Unknown/Irrelevant
		"rescue_unit",
		"inherited",
		"local",
		"police",
		"a",
		"judical",
		"school",
		"college",
		"water",
		"kimmirut",
	}
	_BOUNDARIES = map[string]bool{}
)

func init() {
	for _, key := range _ACCEPTED_BOUNDARIES {
		_BOUNDARIES[key] = true
	}
	for _, key := range _REJECTED_BOUNDARIES {
		_BOUNDARIES[key] = false
	}
}

func copyTags(tags []StringPair) []StringPair {
	other := make([]StringPair, len(tags))
	copy(other, tags)
	return other
}

func patchTags(rel *Relation) []StringPair {
	tags := rel.Tags
	if rel.Id == 937244 {
		// Belgium
		tags = copyTags(tags)
		tags = append(tags,
			StringPair{"ISO3166-1:alpha2", "BE"},
			StringPair{"ISO3166-1:alpha3", "BEL"})
	} else if rel.Id == 1711283 {
		// Jersey
		tags = copyTags(tags)
		tags = append(tags,
			StringPair{"ISO3166-1:alpha2", "JE"},
			StringPair{"ISO3166-1:alpha3", "JEY"})
	} else if rel.Id == 6571872 {
		// Guernsey
		tags = copyTags(tags)
		tags = append(tags,
			StringPair{"ISO3166-1:alpha2", "GG"},
			StringPair{"ISO3166-1:alpha3", "GBG"})
	} else if rel.Id == 2850940 || rel.Id == 4263589 {
		// Philippines
		tags = copyTags(tags)
		tags = append(tags,
			StringPair{"ISO3166-1:alpha2", "PH"},
			StringPair{"ISO3166-1:alpha3", "PHL"})
	}

	return tags
}

func ignoreRelation(rel *Relation) (bool, error) {
	rt, err := NewRelationTags(rel)
	if err != nil {
		return true, err
	}
	switch rel.Id {
	case 2202162, 11980:
		// France has 2 representation, with and without water areas. Let's
		// keep the second one (11980).
		return rel.Id != 11980, nil
	case 1401905:
		// Tuamotu-Gambier(1401905)[level=7]
		// Crashes indexlocations somewhere in a geos finalizer
		return true, nil
	case 62781, 51477:
		// Germany has 3 relations of admin_level=2
		// 51477: outer ways without linestrings
		// 62781: landmass only (no water area)
		// 111111: outer ways with linestring
		// Let's keep the last one for no special reason but we have to pick
		// one.
		return true, nil
	case 1124039:
		// Monaco has 2 representations, with and without water areas. Keep the
		// one without water areas (36990)
		return true, nil
	case 936128:
		// Poland, we used to keep 936128 because it had only land areas but
		// 49715 has more attributes and seems to be more maintained.
		return true, nil
	case 52411:
		// Belgium, keep the land mass (937244)
		// TODO: keep the other, the tags are more interesting
		return true, nil
	case 1711283:
		// Ignore Jersey land area
		return true, nil
	case 270009:
		// Keep Guernsey land mass (6571872)
		return true, nil
	case 2850940, 4263589:
		// Ignore Philippines maritime boundary and continental shell. Keep 443174.
		return true, nil
	case 5441968:
		// Sahrawi Arab Democratic Republic, disputed, no iso code, ignore it
		return true, nil
	case 3263728:
		// British Sovereign Base Areas, disputed, ignore them
		return true, nil
	case 6858045:
		// Liberland, because it does not really exist
		return true, nil
	}
	typ := rt.Tag("type")
	if typ == "collection" || typ == "multilinestring" {
		return true, nil
	}
	level, _ := rt.AdminLevel()
	if level < 1 || level > 8 {
		placeType := rt.PlaceType()
		if placeType != "city" && placeType != "town" {
			return true, nil
		}
	}
	if rt.Name() == "" {
		return true, nil
	}
	boundary := strings.ToLower(rt.Tag("boundary"))
	if len(boundary) > 0 {
		accepted, found := _BOUNDARIES[boundary]
		if !found {
			return true, fmt.Errorf("unknown boundary value for %s: %s",
				rel.String(), boundary)
		}
		if !accepted {
			return true, nil
		}
	}
	return false, nil
}

func buildLocation(rel *Relation, db *WaysDb) (*Location, error) {
	if ok, err := ignoreRelation(rel); ok || err != nil {
		return nil, err
	}
	polygons, err := buildSpecialRelations(rel, db)
	if err != nil {
		return nil, err
	}
	if polygons == nil {
		polygons, err = buildRelationPolygons(rel, db)
		if err != nil {
			return nil, err
		}
	}
	loc, err := polygonsToJson(polygons)
	if err != nil {
		return nil, err
	}
	if loc == nil {
		return nil, nil
	}
	err = db.PutLocation(rel.Id, loc)
	return loc, err
}

func buildRelation(rel *Relation, db *WaysDb) (
	*RelationJson, error) {

	loc, err := db.GetLocation(rel.Id)
	if err != nil {
		return nil, err
	}
	if loc == nil {
		return nil, nil
	}
	center, err := db.GetCentroid(rel.Id)
	if err != nil {
		return nil, err
	}
	if center == nil {
		return nil, nil
	}
	return makeJsonRelation(rel, center, loc)
}
