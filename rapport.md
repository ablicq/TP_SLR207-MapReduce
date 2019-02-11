---
title: Rapport TP SLR207
author: Aurélien Blicq
geometry: margin=2cm
---

# I- programme séquentiel

1. Un HashMap est la structure la plus adaptée car elle permet de lier des mots (objets de type String) à un nombre d'occurences (objets de type Integer)
8. Avec le fichier *sante\_publique.txt*, on a un temps de calcul de 2.5 secondes
9. Avec le fichier de pages web, on a un temps de calcul de 65 secondes

\newpage
# II- ordinateurs en réseau

10. En tapant la commande `nslookup`, on peut récupérer le long d'un ordinateur à partir de son nom court.

Example:
```(bash)
$ nslookup c45-01

Server:		137.194.2.16
Address:	137.194.2.16#53

Name:	c45-01.enst.fr
Address: 137.194.34.192
```

Avec mon ordinateur personel:

```(bash)
$ nslookup pegASUS

Server:		137.194.2.16
Address:	137.194.2.16#53

** server can't find pegASUS: NXDOMAIN
```

Ce qui signifie que mon ordinateur n'appartient à aucun domaine.

11. En utilisant la commande `ifconfig` on obtient les addresses IP de l'ordinateur.

```(bash)
$ ifconfig

lo: flags=73<UP,LOOPBACK,RUNNING>  mtu 65536
        inet 127.0.0.1  netmask 255.0.0.0
        inet6 ::1  prefixlen 128  scopeid 0x10<host>
        loop  txqueuelen 1000  (Local Loopback)
        RX packets 2198  bytes 156733 (153.0 KiB)
        RX errors 0  dropped 0  overruns 0  frame 0
        TX packets 2198  bytes 156733 (153.0 KiB)
        TX errors 0  dropped 0 overruns 0  carrier 0  collisions 0

wlp2s0: flags=4163<UP,BROADCAST,RUNNING,MULTICAST>  mtu 1500
        inet 137.194.92.119  netmask 255.255.248.0  broadcast 137.194.95.255
        inet6 fe80::bbd4:a144:a3e8:b535  prefixlen 64  scopeid 0x20<link>
        inet6 2001:660:330f:16:29d8:420d:5d31:2ac0  prefixlen 64  scopeid 0x0<global>
        ether f8:94:c2:29:6e:db  txqueuelen 1000  (Ethernet)
        RX packets 65049  bytes 74310406 (70.8 MiB)
        RX errors 0  dropped 0  overruns 0  frame 0
        TX packets 13379  bytes 2480537 (2.3 MiB)
        TX errors 0  dropped 0 overruns 0  carrier 0  collisions 0
```

On peut voir par exemple que l'addresse IPv4 de l'ordinateur est 137.194.92.119. On peut également voir l'addresse IPv6, etc.

On peut aussi avoir ces informations sur de nombreux sites internet comme *www.addresseip.com*, *www.mon-ip.com* ou *www.localiser-ip.com*.

14. on fait un ping sur un des ordinateurs de l'école et on obtient le résultat suivant:

```(bash)
$ ping -c 10 c45-19

PING c45-19.enst.fr (137.194.34.210) 56(84) bytes of data.
64 bytes from c45-19.enst.fr (137.194.34.210): icmp_seq=1 ttl=64 time=1.53 ms
64 bytes from c45-19.enst.fr (137.194.34.210): icmp_seq=2 ttl=63 time=1.17 ms
64 bytes from c45-19.enst.fr (137.194.34.210): icmp_seq=3 ttl=63 time=4.97 ms
64 bytes from c45-19.enst.fr (137.194.34.210): icmp_seq=4 ttl=63 time=3.39 ms
64 bytes from c45-19.enst.fr (137.194.34.210): icmp_seq=5 ttl=63 time=3.57 ms
64 bytes from c45-19.enst.fr (137.194.34.210): icmp_seq=6 ttl=63 time=2.83 ms
64 bytes from c45-19.enst.fr (137.194.34.210): icmp_seq=7 ttl=63 time=4.25 ms
64 bytes from c45-19.enst.fr (137.194.34.210): icmp_seq=8 ttl=63 time=3.94 ms
64 bytes from c45-19.enst.fr (137.194.34.210): icmp_seq=9 ttl=63 time=4.15 ms
64 bytes from c45-19.enst.fr (137.194.34.210): icmp_seq=10 ttl=63 time=3.91 ms

--- c45-19.enst.fr ping statistics ---
10 packets transmitted, 10 received, 0% packet loss, time 36ms
rtt min/avg/max/mdev = 1.174/3.372/4.966/1.144 ms
```

16. pour faire de l'arithmetique, on peur par example utiliser `expr`, qui donne un résultat immédiat après un seul appui sur \<Entrée\>.

```(bash)
$ expr 2 + 3

5
```

17. en utilisant une connexion ssh, on peut demander à une machine distante de faire ce calcul (un mot de passe est nécessaire.

18. la commande suivante permet d'enregistrer sa clé ssh public sur le server de l'école et de ne pas avoir à entrer de mot de passe pour l'authentification : \newline
`cat ~/.ssh/id_rsa.pub | ssh ablicq@ssh.enst.fr 'cat >> .ssh/authorized_keys'`
