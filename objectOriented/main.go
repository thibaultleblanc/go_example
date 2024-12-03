package main

import (
	"personnage"
)

func main() {
	personnage := personnage.NewPersonnage("Bob", 100, 200)
	personnage.Affichage()
}
