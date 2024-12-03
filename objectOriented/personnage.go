package personnage

import "fmt"

type Personnage struct {
	Nom        string
	Vie        float
	Puissance  int
	Mort       bool
	Inventaire [3]string
}

func New(nom string, vie float, puissance int) Personnage {
	personnage := Personnage(nom, vie, puissance, false, [3]string{})
	return personnage
}

func (p Personnage) Affichage() {
	fmt.Println("-----------------------------------")
	fmt.Println("Mort", p.Mort)
	fmt.Println("Nom", p.Nom)
	fmt.Println("Vie", p.Vie)
	fmt.Println("Puissance", p.Puissance)
	fmt.Println("Inventaire", p.Inventaire)
	fmt.Println("-----------------------------------")
}
