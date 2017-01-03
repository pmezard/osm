package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

type Element struct {
	Id       int64
	Name     string
	Children []*Element
	Tags     []StringPair
}

func getTag(tags []StringPair, key string) string {
	for _, tag := range tags {
		if key == tag.Key {
			return tag.Value
		}
	}
	return ""
}

func traverse(rootId int64, tree map[int64]*Element, depth int) {
	n, ok := tree[rootId]
	if !ok || n.Name == "" {
		return
	}
	if getTag(n.Tags, "type") == "boundary" {
		fmt.Println(strings.Repeat(" ", depth), rootId, n.Name, getTag(n.Tags, "admin_level"))
	}
	for _, tag := range n.Tags {
		if strings.HasPrefix(tag.Key, "name:") {
			continue
		}
		if tag.Key == "name" {
			// fmt.Println(strings.Repeat(" ", depth+2), tag.Key, tag.Value)
		}
	}
	for _, c := range n.Children {
		traverse(c.Id, tree, depth+1)
	}
}

func parse() error {
	flag.Parse()
	if flag.NArg() != 1 {
		return fmt.Errorf("one filename expected")
	}
	path := flag.Arg(0)
	r, err := NewO5MReader(path)
	if err != nil {
		return err
	}

	tree := map[int64]*Element{}
	children := map[int64]bool{}
	for r.Next() {
		if r.Kind() == BBoxKind {
			bb := r.BoundingBox()
			fmt.Printf("BBOX %f %f %f %f\n", bb.X1, bb.Y1, bb.X2, bb.Y2)
		} else if r.Kind() == NodeKind {
			r.Node()
			// fmt.Println("NODE", n.Id, n.Lon, n.Lat)
		} else if r.Kind() == WayKind {
			r.Way()
			// fmt.Println("NODE", n.Id, n.Lon, n.Lat)
		} else if r.Kind() == RelationKind {
			rel := r.Relation()
			n := &Element{
				Id: rel.Id,
			}
			//fmt.Println("RELATION", rel.Id)
			for _, ref := range rel.Refs {
				c, ok := tree[ref.Id]
				if !ok {
					c = &Element{
						Id: ref.Id,
					}
					tree[ref.Id] = c
				}
				n.Children = append(n.Children, c)
				children[ref.Id] = true
			}
			for _, tag := range rel.Tags {
				if tag.Key == "name" {
					n.Name = tag.Value
				}
				n.Tags = append(n.Tags, tag)
			}
			tree[n.Id] = n
		}
	}
	if r.Err() != nil {
		return r.Err()
	}
	for _, n := range tree {
		if _, ok := children[n.Id]; ok {
			continue
		}
		/*
			if getTag(n.Tags, "admin_level") != "2" {
				continue
			}
		*/
		traverse(n.Id, tree, 0)
	}
	return nil
}

func scan() error {
	flag.Parse()
	if flag.NArg() != 1 {
		return fmt.Errorf("one filename expected")
	}
	path := flag.Arg(0)
	r, err := NewO5MReader(path)
	if err != nil {
		return err
	}

	nodes := 0
	ways := 0
	relations := 0
	for r.Next() {
		if r.Kind() == NodeKind {
			nodes += 1
		} else if r.Kind() == WayKind {
			ways += 1
		} else if r.Kind() == RelationKind {
			relations += 1
		}
	}
	if r.Err() != nil {
		return r.Err()
	}
	fmt.Println("nodes", nodes)
	fmt.Println("ways", ways)
	fmt.Println("relations", relations)
	return nil
}

func main() {
	err := scan()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n", err)
		os.Exit(1)
	}
}
