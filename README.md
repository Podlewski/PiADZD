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