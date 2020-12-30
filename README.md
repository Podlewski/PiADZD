# PiADZD

<img src="https://static.dwcdn.net/css/flag-icons/flags/4x3/pl.svg" height="10" width="20"> Laboratoria z **Przetwarzania i analizy dużych zbiorów danych** na Politechnice Łódzkiej (PŁ). Więcej informacji o przedmiocie: [karta przedmiotu](https://programy.p.lodz.pl/ectslabel-web/przedmiot_3.jsp?l=pl&idPrzedmiotu=172836&pkId=1149&s=2&j=0&w=informatyka%20stosowana&v=3).

<img src="https://static.dwcdn.net/css/flag-icons/flags/4x3/gb.svg" height="10" width="20"> **Big Data Processing And Analysis** classes at Lodz University of Technology (TUL).

---

## Zadanie 1

> Pobierz dane dotyczące zgłoszeń do władz Nowego Jorku za pośrednictwem numeru 3-1-1: https://nycopendata.socrata.com/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9

1. Na podstawie pliku CSV znajdź:
    - najczęściej zgłaszane skargi,
    - najczęściej zgłaszane skargi w każdej dzielnicy,
    - urzędy, do których najczęściej zgłaszano skargi.

    Zmierz czasy wykonania kwerend.

2. Wczytaj dane z pliku CSV do bazy danych SQL i ponownie wykonaj trzy powyższe kwerendy.
    Zmierz czasy wykonania kwerend.
    Zmierz łączny czas potrzebny na przekonwertowanie danych oraz wykonanie kwerend.
    Porównaj te czasy dla dwóch różnych baz danych (np. MySQL oraz SQLite).

    > **Ważne!** Do łączenia się z bazą danych oraz wykonania kwerend wykorzystaj język Python.

3. W jaki sposób można zredukować czas wykonania kwerend? Zaimplementuj własne rozwiązanie i przedstaw wyniki.

    > **Ważne!** Wykonaj zadanie również wtedy, gdy osiągane rezultaty nie są lepsze od wyjściowych.

## Zadanie 2

Napisz program, który implementuje znany z mediów społecznościowych algorytm "Osoby, które możesz znać". Powinien on działać na zasadzie, że jeżeli dwóch użytkowników ma wielu wspólnych znajomych, to system proponuje im znajomość.

> **Ważne!** W tym celu skorzystaj z Apache Spark oraz języka Python.

_Algorytm_
- dla każdego użytkownika program powinien polecić mu 10 nowych użytkowników, którzy nie są jego znajomymi, ale którzy mają z nim najwięcej wspólnych znajomych.

_Plik wejściowy_
- plik zawiera linie w formacie <UŻYTKOWNIK><TABULATOR><ZNAJOMI>, gdzie <UŻYTKOWNIK> oznacza unikalny identyfikator użytkownika, a <ZNAJOMI> to oddzielone po przecinku identyfikatory znajomych użytkownika o identyfikatorze <UŻYTKOWNIK>.

_Plik wyjściowy_
- plik powinien zawierać linie w formacie <UŻYTKOWNIK><TABULATOR><REKOMENDACJE>, gdzie <UŻYTKOWNIK> oznacza unikalny identyfikator użytkownika, a <REKOMENDACJE> to oddzielone po przecinku identyfikatory użytkowników, którzy są rekomendowani jako znajomi użytkownikowi o identyfikatorze <UŻYTKOWNIK>.

    > **Ważne!** Rekomendacje powinny zostać posortowane malejąco według liczby wspólnych znajomych. Jeżeli rekomendowani użytkownicy mają tylu samych wspólnych znajomych, to należy ich posortować rosnąco według ich identyfikatora. Rekomendacje należy przedstawić również wtedy, gdy jest ich mniej niż 10. Jeżeli użytkownik nie ma znajomych, to wynikiem powinna być pusta lista.

Opisz w maksymalnie 3 zdaniach, w jaki sposób skorzystałeś z Apache Spark, aby rozwiązać zadanie.

Przedstaw rekomendacje dla użytkowników o identyfikatorach: 924, 8941, 8942, 9019, 9020, 9021, 9022, 9990, 9992, 9993.

> **Ważne!** Przydatne może okazać się polecenie .take(X).

> **Podpowiedź:** rekomendacje dla użytkownika o identyfikatorze 11 to: 27552, 7785, 27573, 27574, 27589, 27590, 27600, 27617, 27620, 27667.

## Zadanie 3

1. Napisz program, który implementuje algorytm k-średnich z uwzględnieniem dwóch miar: euklidesowej oraz Manhattan.

> **Ważne!** W tym celu skorzystaj z Apache Spark oraz języka Python.

Miara euklidesowa
- odległość: ||a−b|| = sqrt(∑di=1(a_i−b_i) ^ 2)
- koszt: ϕ=∑x∈Xminc∈C||x−c|| ^ 2

Miara Manhattan
- odległość: |a−b|=∑di=1|a_i−b_i|
- koszt: ψ=∑x∈Xminc∈C|x−c|

Pseudokod
```
procedure Iteracyjny algorytm k-średnich
    Wybierz k wektorów jako początkowe centroidy k klastrów.
    for iteracje := 1 to MAX_ITERACJI do
        for każdy wektor x do
            Przypisz wektor x do klastra o najbliższym centroidzie.
        end for
        Oblicz koszt iteracji.
        for każdy klaster c do
            Oblicz nowy centroid klastra c jako średnią ze wszystkich wektorów do niego przypisanych.
        end for
    end for
end procedure
```

> **Ważne!** Algorytm należy zaimplementować samodzielnie. Nie wolno korzystać z gotowych rozwiązań.

Pliki wejściowe
- plik 3a.txt zawiera zbiór danych, który posiada 4601 wektorów o 58 kolumnach
- plik 3b.txt zawiera 10 początkowych centroidów, które zostały losowo wybrane spośród zbioru danych
- plik 3c.txt zawiera 10 początkowych centroidów, które są od siebie maksymalnie oddalone według odległości euklidesowej

Parametry
- liczba klastrów k powinna liczyć 10
- maksymalna liczba iteracji MAX_ITERACJI ma wynosić 20

2. Dla obydwu początkowych rozmieszczeń centroidów oblicz funkcje kosztu ϕ(i) oraz ψ(i) dla każdej iteracji i.

3. Wygeneruj wykresy ϕ(i) oraz ψ(i). Tutaj również uwzględnij obydwa początkowe rozmieszczenia centroidów.

4. Jak procentowo zmienił się koszt po 10 iteracjach algorytmu dla obydwu miar odległości? Dla którego z początkowych rozmieszczeń centroidów uzyskano lepsze rezultaty?

> **Ważne!** W tym celu podziel różnicę między kosztem początkowym oraz tym uzyskanym po 10. iteracji przez koszt początkowy.

## Zadanie 4

Napisz program, który implementuje reguły asocjacyjne za pomocą algorytmu A-priori. Program powinien - na podstawie ostatnio przeglądanych przez użytkowników przedmiotów - identyfikować te, które regularnie występowały wspólnie w ramach pojedynczej sesji.

> **Ważne!** W tym celu skorzystaj z Apache Spark oraz języka Python.

Miary istotności
Załóżmy, że X i Y to zbiory elementów transakcji, t oznacza transakcję, a T jest zbiorem transakcji.
- wsparcie (ang. support): supp(X)=|{t∈T:X⊆t}||T|
- ufność (ang. confidence): conf(X⇒Y)=supp(X∪Y)supp(X)

Algorytm
- program powinien znaleźć wszystkie pary i trójki przedmiotów, które wystąpiły wspólnie w ramach pojedynczej sesji co najmniej 100 razy
- dla znalezionych grup przedmiotów należy obliczyć ufność dla wszystkich możliwych do utworzenia reguł asocjacyjnych,
np. X⇒Y i Y⇒X oraz (X,Y)⇒Z, (X,Z)⇒Y i (Y,Z)⇒X

> **Ważne!** Reguły powinny zostać posortowane malejąco według ufności. W przypadku uzyskania tych samych wartości należy zastosować porządek leksykograficzny według poprzedników.

Plik wejściowy
- każda linia w pliku reprezentuje pojedynczą sesję użytkownika, a rozdzielone spacjami ośmioznakowe sygnatury to identyfikatory przeglądanych produktów

> **Ważne!** Algorytm należy zaimplementować samodzielnie. Nie wolno korzystać z gotowych rozwiązań.

2. Przedstaw po 5 reguł asocjacyjnych o największej ufności dla par i trójek produktów.

> **Podpowiedzi**:
> - istnieje 647 jednoelementowych zbiorów, które występują co najmniej 100 razy
> - każda z 5 oczekiwanych reguł asocjacyjnych dla par produktów ma ufność większą niż 0.985

## Projekt

Celem projektu jest potwierdzenie wiedzy i umiejętności z zakresu przetwarzania i analizy danych masowych poprzez przeprowadzenie analizy danych na zaproponowany temat z wykorzystaniem rozwiązań charakterystycznych dla dużych zbiorów danych.

W ramach zadań projektowych należy:

wybrać zbiór danych,
dobrać odpowiednie metody analizy danych i uzasadnić ich wybór,
zaimplementować metody,
przeprowadzić obliczenia,
przedstawić i omówić wyniki.
Ważne! Warunkiem uzyskania zaliczenia jest wskazanie co najmniej dwóch aspektów projektu, w których zastosowano rozwiązania charakterystyczne dla dużych zbiorów danych (zbiór danych, metoda analizy danych, technologia).

Projekt ma charakter grupowy i powinien zostać zrealizowany w trzy lub cztery osoby.

Harmonogram
09/12/2020    20% oceny    prezentacje wstępne
05/01/2021    30% oceny    prezentacje kontrolne
20/01/2021    50% oceny    prezentacje końcowe

Prezentacje wstępne
Prezentacje powinny trwać maksymalnie 5 minut. Po każdej z nich zaplanowana jest dyskusja.

Wymagania:
- nazwa projektu
- imiona i nazwiska członków grupy wraz ze wskazaniem jej lidera
- motywacja skłaniająca do wyboru tematyki projektu
- cel naukowy oraz dodatkowe cele projektu
- zbiór danych
- metody analizy danych
- technologie i języki programowania

Ważne! Propozycja projektu może ulec modyfikacji do czasu prezentacji kontrolnych.

Prezentacje kontrolne
Prezentacje powinny trwać maksymalnie 10 minut. Po każdej z nich zaplanowana jest dyskusja.

Prezentacje końcowe
Prezentacje powinny trwać maksymalnie 15 minut. Po każdej z nich zaplanowana jest dyskusja.