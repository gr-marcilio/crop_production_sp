with raw_data_area_plantada as (
    select
        coalesce(try_cast(regexp_replace(cast("Ano" as varchar), '[^0-9]', '') as int), 0) as ano
        , coalesce(try_cast(regexp_replace(cast("Total" as varchar), '[^0-9]', '') as bigint), 0) as total
        , coalesce(try_cast(regexp_replace(cast("Abacate" as varchar), '[^0-9]', '') as bigint), 0) as abacate
        , coalesce(try_cast(regexp_replace(cast("Abacaxi*" as varchar), '[^0-9]', '') as bigint), 0) as abacaxi
        , coalesce(try_cast(regexp_replace(cast("Açaí" as varchar), '[^0-9]', '') as bigint), 0) as acai
        , coalesce(try_cast(regexp_replace(cast("Alfafa fenada" as varchar), '[^0-9]', '') as bigint), 0) as alfafa_fenada
        , coalesce(try_cast(regexp_replace(cast("Algodão arbóreo (em caroço)" as varchar), '[^0-9]', '') as bigint), 0) as algodao_arboreo_em_caroco
        , coalesce(try_cast(regexp_replace(cast("Algodão herbáceo (em caroço)" as varchar), '[^0-9]', '') as bigint), 0) as algodao_herbaceo_em_caroco
        , coalesce(try_cast(regexp_replace(cast("Alho" as varchar), '[^0-9]', '') as bigint), 0) as alho
        , coalesce(try_cast(regexp_replace(cast("Amendoim (em casca)" as varchar), '[^0-9]', '') as bigint), 0) as amendoim_em_casca
        , coalesce(try_cast(regexp_replace(cast("Arroz (em casca)" as varchar), '[^0-9]', '') as bigint), 0) as arroz_em_casca
        , coalesce(try_cast(regexp_replace(cast("Aveia (em grão)" as varchar), '[^0-9]', '') as bigint), 0) as aveia_em_grao
        , coalesce(try_cast(regexp_replace(cast("Azeitona" as varchar), '[^0-9]', '') as bigint), 0) as azeitona
        , coalesce(try_cast(regexp_replace(cast("Banana (cacho)" as varchar), '[^0-9]', '') as bigint), 0) as banana_cacho
        , coalesce(try_cast(regexp_replace(cast("Batata-doce" as varchar), '[^0-9]', '') as bigint), 0) as batata_doce
        , coalesce(try_cast(regexp_replace(cast("Batata-inglesa" as varchar), '[^0-9]', '') as bigint), 0) as batata_inglesa
        , coalesce(try_cast(regexp_replace(cast("Borracha (látex coagulado)" as varchar), '[^0-9]', '') as bigint), 0) as borracha_latex_coagulado
        , coalesce(try_cast(regexp_replace(cast("Borracha (látex líquido)" as varchar), '[^0-9]', '') as bigint), 0) as borracha_latex_liquido
        , coalesce(try_cast(regexp_replace(cast("Cacau (em amêndoa)" as varchar), '[^0-9]', '') as bigint), 0) as cacau_em_amendoa
        , coalesce(try_cast(regexp_replace(cast("Café (em grão) Total" as varchar), '[^0-9]', '') as bigint), 0) as cafe_em_grao_total
        , coalesce(try_cast(regexp_replace(cast("Café (em grão) Arábica" as varchar), '[^0-9]', '') as bigint), 0) as cafe_em_grao_arabica
        , coalesce(try_cast(regexp_replace(cast("Café (em grão) Canephora" as varchar), '[^0-9]', '') as bigint), 0) as cafe_em_grao_canephora
        , coalesce(try_cast(regexp_replace(cast("Caju" as varchar), '[^0-9]', '') as bigint), 0) as caju
        , coalesce(try_cast(regexp_replace(cast("Cana-de-açúcar" as varchar), '[^0-9]', '') as bigint), 0) as cana_de_acucar
        , coalesce(try_cast(regexp_replace(cast("Cana para forragem" as varchar), '[^0-9]', '') as bigint), 0) as cana_para_forragem
        , coalesce(try_cast(regexp_replace(cast("Caqui" as varchar), '[^0-9]', '') as bigint), 0) as caqui
        , coalesce(try_cast(regexp_replace(cast("Castanha de caju" as varchar), '[^0-9]', '') as bigint), 0) as castanha_de_caju
        , coalesce(try_cast(regexp_replace(cast("Cebola" as varchar), '[^0-9]', '') as bigint), 0) as cebola
        , coalesce(try_cast(regexp_replace(cast("Centeio (em grão)" as varchar), '[^0-9]', '') as bigint), 0) as centeio_em_grao
        , coalesce(try_cast(regexp_replace(cast("Cevada (em grão)" as varchar), '[^0-9]', '') as bigint), 0) as cevada_em_grao
        , coalesce(try_cast(regexp_replace(cast("Chá-da-índia (folha verde)" as varchar), '[^0-9]', '') as bigint), 0) as cha_da_india_folha_verde
        , coalesce(try_cast(regexp_replace(cast("Coco-da-baía*" as varchar), '[^0-9]', '') as bigint), 0) as coco_da_baia
        , coalesce(try_cast(regexp_replace(cast("Dendê (cacho de coco)" as varchar), '[^0-9]', '') as bigint), 0) as dende_cacho_de_coco
        , coalesce(try_cast(regexp_replace(cast("Erva-mate (folha verde)" as varchar), '[^0-9]', '') as bigint), 0) as erva_mate_folha_verde
        , coalesce(try_cast(regexp_replace(cast("Ervilha (em grão)" as varchar), '[^0-9]', '') as bigint), 0) as ervilha_em_grao
        , coalesce(try_cast(regexp_replace(cast("Fava (em grão)" as varchar), '[^0-9]', '') as bigint), 0) as fava_em_grao
        , coalesce(try_cast(regexp_replace(cast("Feijão (em grão)" as varchar), '[^0-9]', '') as bigint), 0) as feijao_em_grao
        , coalesce(try_cast(regexp_replace(cast("Figo" as varchar), '[^0-9]', '') as bigint), 0) as figo
        , coalesce(try_cast(regexp_replace(cast("Fumo (em folha)" as varchar), '[^0-9]', '') as bigint), 0) as fumo_em_folha
        , coalesce(try_cast(regexp_replace(cast("Girassol (em grão)" as varchar), '[^0-9]', '') as bigint), 0) as girassol_em_grao
        , coalesce(try_cast(regexp_replace(cast("Goiaba" as varchar), '[^0-9]', '') as bigint), 0) as goiaba
        , coalesce(try_cast(regexp_replace(cast("Guaraná (semente)" as varchar), '[^0-9]', '') as bigint), 0) as guarana_semente
        , coalesce(try_cast(regexp_replace(cast("Juta (fibra)" as varchar), '[^0-9]', '') as bigint), 0) as juta_fibra
        , coalesce(try_cast(regexp_replace(cast("Laranja" as varchar), '[^0-9]', '') as bigint), 0) as laranja
        , coalesce(try_cast(regexp_replace(cast("Limão" as varchar), '[^0-9]', '') as bigint), 0) as limao
        , coalesce(try_cast(regexp_replace(cast("Linho (semente)" as varchar), '[^0-9]', '') as bigint), 0) as linho_semente
        , coalesce(try_cast(regexp_replace(cast("Maçã" as varchar), '[^0-9]', '') as bigint), 0) as maca
        , coalesce(try_cast(regexp_replace(cast("Malva (fibra)" as varchar), '[^0-9]', '') as bigint), 0) as malva_fibra
        , coalesce(try_cast(regexp_replace(cast("Mamão" as varchar), '[^0-9]', '') as bigint), 0) as mamao
        , coalesce(try_cast(regexp_replace(cast("Mamona (baga)" as varchar), '[^0-9]', '') as bigint), 0) as mamona_baga
        , coalesce(try_cast(regexp_replace(cast("Mandioca" as varchar), '[^0-9]', '') as bigint), 0) as mandioca
        , coalesce(try_cast(regexp_replace(cast("Manga" as varchar), '[^0-9]', '') as bigint), 0) as manga
        , coalesce(try_cast(regexp_replace(cast("Maracujá" as varchar), '[^0-9]', '') as bigint), 0) as maracuja
        , coalesce(try_cast(regexp_replace(cast("Marmelo" as varchar), '[^0-9]', '') as bigint), 0) as marmelo
        , coalesce(try_cast(regexp_replace(cast("Melancia" as varchar), '[^0-9]', '') as bigint), 0) as melancia
        , coalesce(try_cast(regexp_replace(cast("Melão" as varchar), '[^0-9]', '') as bigint), 0) as melao
        , coalesce(try_cast(regexp_replace(cast("Milho (em grão)" as varchar), '[^0-9]', '') as bigint), 0) as milho_em_grao
        , coalesce(try_cast(regexp_replace(cast("Noz (fruto seco)" as varchar), '[^0-9]', '') as bigint), 0) as noz_fruto_seco
        , coalesce(try_cast(regexp_replace(cast("Palmito" as varchar), '[^0-9]', '') as bigint), 0) as palmito
        , coalesce(try_cast(regexp_replace(cast("Pera" as varchar), '[^0-9]', '') as bigint), 0) as pera
        , coalesce(try_cast(regexp_replace(cast("Pêssego" as varchar), '[^0-9]', '') as bigint), 0) as pessego
        , coalesce(try_cast(regexp_replace(cast("Pimenta-do-reino" as varchar), '[^0-9]', '') as bigint), 0) as pimenta_do_reino
        , coalesce(try_cast(regexp_replace(cast("Rami (fibra)" as varchar), '[^0-9]', '') as bigint), 0) as rami_fibra
        , coalesce(try_cast(regexp_replace(cast("Sisal ou agave (fibra)" as varchar), '[^0-9]', '') as bigint), 0) as sisal_ou_agave_fibra
        , coalesce(try_cast(regexp_replace(cast("Soja (em grão)" as varchar), '[^0-9]', '') as bigint), 0) as soja_em_grao
        , coalesce(try_cast(regexp_replace(cast("Sorgo (em grão)" as varchar), '[^0-9]', '') as bigint), 0) as sorgo_em_grao
        , coalesce(try_cast(regexp_replace(cast("Tangerina" as varchar), '[^0-9]', '') as bigint), 0) as tangerina
        , coalesce(try_cast(regexp_replace(cast("Tomate" as varchar), '[^0-9]', '') as bigint), 0) as tomate
        , coalesce(try_cast(regexp_replace(cast("Trigo (em grão)" as varchar), '[^0-9]', '') as bigint), 0) as trigo_em_grao
        , coalesce(try_cast(regexp_replace(cast("Triticale (em grão)" as varchar), '[^0-9]', '') as bigint), 0) as triticale_em_grao
        , coalesce(try_cast(regexp_replace(cast("Tungue (fruto seco)" as varchar), '[^0-9]', '') as bigint), 0) as tungue_fruto_seco
        , coalesce(try_cast(regexp_replace(cast("Urucum (semente)" as varchar), '[^0-9]', '') as bigint), 0) as urucum_semente
        , coalesce(try_cast(regexp_replace(cast("Uva" as varchar), '[^0-9]', '') as bigint), 0) as uva
    from {{ ref('area_plantada_sp') }}
)

select 
    *
from raw_data_area_plantada
