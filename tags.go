package edb

import "sync/atomic"

var lastTagIndex atomic.Int64
var lastTagFamilyIndex atomic.Int64

type Tag struct {
	index int
	name  string
}

func NewTag(name string) *Tag {
	return &Tag{
		index: int(lastTagIndex.Add(1)),
		name:  name,
	}
}

func (tag *Tag) String() string { return tag.name }
func (tag *Tag) Name() string   { return tag.name }

type TagFamilyCardinality int

const (
	TagFamilyCardinalityOptionalOne TagFamilyCardinality = iota
	TagFamilyCardinalityRequiredOne
	TagFamilyCardinalityOptionalMany
	TagFamilyCardinalityRequiredMany
)

type TagFamily struct {
	index      int
	name       string
	tags       []*Tag
	defaultTag *Tag
	card       TagFamilyCardinality
}

func NewTagFamily(name string, tags []*Tag, defaultTag *Tag, card TagFamilyCardinality) *TagFamily {
	return &TagFamily{
		index:      int(lastTagFamilyIndex.Add(1)),
		name:       name,
		tags:       tags,
		defaultTag: defaultTag,
		card:       card,
	}
}

func (fam *TagFamily) String() string { return fam.name }
func (fam *TagFamily) Name() string   { return fam.name }

type TaggableImpl struct {
	tags []*Tag
}

func (i *TaggableImpl) HasTag(tag *Tag) bool {
	for _, t := range i.tags {
		if t == tag {
			return true
		}
	}
	return false
}

func (i *TaggableImpl) TagInFamily(fam *TagFamily) *Tag {
	for _, tag := range fam.tags {
		for _, t := range i.tags {
			if t == tag {
				return t
			}
		}
	}
	return nil
}
