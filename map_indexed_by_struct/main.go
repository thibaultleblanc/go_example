package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type SortedListOfNode struct {
	key          string
	idListOfNode []int
}

func (s *SortedListOfNode) AddNode(id int) {
	s.idListOfNode = append(s.idListOfNode, id)
	sort.Ints(s.idListOfNode)
}

func IntSliceToString(slice []int) string {
	strSlice := make([]string, len(slice))
	for i, v := range slice {
		strSlice[i] = strconv.Itoa(v)
	}
	return strings.Join(strSlice, ",")
}

func main() {
	// Création de la map avec SortedListOfNode comme clé et nom de scientifique comme valeur
	nodeLists := make([]SortedListOfNode, 0)
	scientists := make(map[string]string)

	// Exemple d'utilisation
	nodeList1 := SortedListOfNode{"nodeList1", []int{}}
	nodeList1.AddNode(3)
	nodeList1.AddNode(1)
	nodeList1.AddNode(2)

	nodeList2 := SortedListOfNode{"nodeList2", []int{}}
	nodeList2.AddNode(5)
	nodeList2.AddNode(4)

	nodeLists = append(nodeLists, nodeList1)
	nodeLists = append(nodeLists, nodeList2)

	scientists["nodeList1"] = "Albert Einstein"
	scientists["nodeList2"] = "Isaac Newton"

	for _, nodeList := range nodeLists {
		fmt.Printf("Scientist: %s, Nodes: %s\n", scientists[nodeList.key], IntSliceToString(nodeList.idListOfNode))
	}
}
