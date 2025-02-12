WITH tb_base AS (
  SELECT distinct v.idVendedor, ip.idPedido, p.dtPedido, ap.idAvaliacao
  FROM silver.olist.vendedor as v
  LEFT JOIN silver.olist.item_pedido as ip ON v.idVendedor = ip.idVendedor
  LEFT JOIN silver.olist.pedido as p ON ip.idPedido = p.idPedido
  LEFT JOIN silver.olist.avaliacao_pedido as ap ON ip.idPedido = ap.idPedido
),
-- Criar a granularidade: Dtref (ANO/MÊS) + id.vendedor
tb_dtref_vendedor AS (
  SELECT DATE_TRUNC('MONTH', dtPedido) AS dtref, idVendedor, idPedido, idAvaliacao
  FROM tb_base
)

SELECT *
FROM tb_dtref_vendedor