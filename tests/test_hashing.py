"""
Tests del anillo de hashing consistente con nodos virtuales ponderados
(common/hashing.py). Cubre:

  - Determinismo: misma entrada -> mismos dueños (Req 3.5).
  - owners_for nunca repite un peer fisico y respeta el factor.
  - Retrocompatibilidad: lista de addresses == dict con peso 1.
  - Distribucion proporcional al peso (Req 3.7): sobre >= 10000 claves,
    la fraccion de claves de cada peer no se desvia mas de 5 puntos
    porcentuales de peso_i / sum(pesos).
  - Pesos invalidos (<=0 / no enteros) se tratan como 1 (defensivo).
"""

from collections import Counter

from common.hashing import Ring


PEERS4 = ["peer1:50051", "peer2:50051", "peer3:50051", "peer4:50051"]


def _keys(n):
    return [f"chunk::/docs/file.bin::{i}" for i in range(n)]


def test_determinismo_mismos_duenos():
    r1 = Ring(PEERS4)
    r2 = Ring(list(reversed(PEERS4)))  # mismo conjunto, orden distinto
    for k in _keys(500):
        assert r1.owners_for(k, 2) == r2.owners_for(k, 2)


def test_owners_no_repite_peer_fisico():
    r = Ring(PEERS4)
    for k in _keys(500):
        owners = r.owners_for(k, 3)
        assert len(owners) == len(set(owners))  # sin duplicados
        assert len(owners) == 3                 # respeta el rf pedido
        for o in owners:
            assert o in PEERS4


def test_rf_acotado_por_numero_de_peers():
    r = Ring(["a:1", "b:1"])
    owners = r.owners_for("alguna-key", 5)  # rf mayor que #peers
    assert len(owners) == 2
    assert set(owners) == {"a:1", "b:1"}


def test_lista_equivale_a_dict_peso_uno():
    r_list = Ring(PEERS4)
    r_dict = Ring({p: 1 for p in PEERS4})
    for k in _keys(500):
        assert r_list.owners_for(k, 2) == r_dict.owners_for(k, 2)


def test_empty_ring():
    r = Ring([])
    assert r.is_empty()
    assert r.owners_for("k", 2) == []
    assert r.primary_owner("k") is None


def test_distribucion_uniforme_pesos_iguales():
    r = Ring(PEERS4)
    counts = Counter(r.primary_owner(k) for k in _keys(20000))
    total = sum(counts.values())
    for p in PEERS4:
        frac = counts[p] / total
        assert abs(frac - 0.25) <= 0.05, f"{p} tiene fraccion {frac:.3f}"


def test_distribucion_proporcional_al_peso():
    # peer_big peso 3, los otros peso 1 -> suma de pesos = 6.
    # fraccion esperada: big=3/6=0.5, cada chico=1/6~=0.167.
    weights = {
        "big:50051": 3,
        "small1:50051": 1,
        "small2:50051": 1,
        "small3:50051": 1,
    }
    r = Ring(weights)
    counts = Counter(r.primary_owner(k) for k in _keys(20000))
    total = sum(counts.values())

    assert abs(counts["big:50051"] / total - 0.5) <= 0.05
    for s in ("small1:50051", "small2:50051", "small3:50051"):
        assert abs(counts[s] / total - (1 / 6)) <= 0.05


def test_vnodes_proporcionales_al_peso():
    r = Ring({"a:1": 1, "b:1": 2})
    # a aporta BASE vnodes, b aporta 2*BASE -> total 3*BASE puntos.
    assert len(r._members) == 3 * Ring.BASE_VIRTUAL_NODES
    assert r.weight_of("a:1") == 1
    assert r.weight_of("b:1") == 2


def test_pesos_invalidos_se_tratan_como_uno():
    r = Ring({"a:1": 0, "b:1": -5, "c:1": "x"})
    assert r.weight_of("a:1") == 1
    assert r.weight_of("b:1") == 1
    assert r.weight_of("c:1") == 1
    assert len(r._members) == 3 * Ring.BASE_VIRTUAL_NODES
