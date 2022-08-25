CREATE MATERIALIZED VIEW nome_backup.nome_da_view
TABLESPACE pg_default
AS SELECT
(
v3.estabelecimento_cnes,
    upper(v3.estabelecimento_nome) AS estabelecimento_nome,
    v3.equipe_ine,
    v3.equipe_nome,
    upper(v3.acs_nome::text) AS acs_nome,
    v3.acs_data_ultima_visita,
    v3.gestante_documento_cpf,
    v3.gestante_documento_cns,
    v3.gestante_nome,
    v3.gestante_telefone,
    v3.gestante_endereco,
    v3.gestante_data_de_nascimento,
    v3.gestante_dum,
    v3.gestante_idade_gestacional AS gestante_idade_gestacional_atual,
	/* Retorna a primeira idade gestacional:
- Caso data de DUM preenchida corretamente (diferente de 3000), retorna (data de atendimento - data da dum)/7
- Pega a primeira gestacional, sendo o campo nu_idade_gestacional_semanas da Ficha de atendimento individual (tb_fat_atendimento_individual), ordenado pelo atendimento 
- Caso ambos estejam vazios, retorna nulo
*/
        CASE
            WHEN date_part('year'::text, v3.gestante_dum) <> 3000::double precision THEN (v3.atendimento_primeiro_data - v3.gestante_dum) / 7
            WHEN v3.gestante_idade_gestacional_primeiro_atendimento IS NOT NULL THEN v3.gestante_idade_gestacional_primeiro_atendimento
            ELSE NULL::integer
        END AS gestante_idade_gestacional_primeiro_atendimento,
    v3.gestante_primeira_dpp AS gestante_dpp,
/* Considera DPP como data de pré-natal limite */
    v3.gestante_primeira_dpp AS gestante_consulta_prenatal_data_limite,
/* Retorna diferença de dias da data da atual (do script rodado) para a da dpp */
    v3.gestante_primeira_dpp::date - CURRENT_DATE AS gestante_dpp_dias_para,
    v3.gestante_consulta_prenatal_total,
    v3.gestante_consulta_prenatal_ultima_data,
    v3.gestante_consulta_prenatal_ultima_dias_desde,
    v3.atendimento_odontologico_realizado,
    v3.exame_hiv_realizado,
    v3.exame_sifilis_realizado,
/* Campo verdadeiro caso campo de exame_sifilis_realizado e exame_sifilis_hiv_realizado verdadeiros */
    v3.exame_hiv_realizado AND v3.exame_sifilis_realizado AS exame_sifilis_hiv_realizado,
        CASE
            WHEN v3.possui_registro_aborto IS TRUE THEN 'Sim'::text
            ELSE 'Não'::text
        END AS possui_registro_aborto,
        CASE
            WHEN v3.possui_registro_parto IS TRUE THEN 'Sim'::text
            ELSE 'Não'::text
        END AS possui_registro_parto
FROM ( SELECT row_number() OVER (PARTITION BY v2.gestante_nome, v2.gestante_data_de_nascimento ORDER BY v2.atendimento_data DESC) AS r,
            v2.atendimento_data,
            v2.atendimento_primeiro_data,
            v2.estabelecimento_cnes,
            v2.estabelecimento_nome,
            v2.equipe_ine,
            v2.equipe_nome,
            v2.acs_nome,
            v2.acs_data_ultima_visita,
            v2.co_fat_cidadao_pec,
            v2.gestante_documento_cpf,
            v2.gestante_documento_cns,
            v2.gestante_nome,
            v2.gestante_telefone,
            v2.gestante_endereco,
            v2.gestante_data_de_nascimento,
            v2.gestante_data_nascimento_ts,
            v2.gestante_idade_gestacional,
            v2.gestante_dum_primeiro_atendimento AS gestante_dum,
            v2.gestante_idade_gestacional_primeiro_atendimento,
            v2.gestante_consulta_prenatal_total,
            v2.gestante_consulta_prenatal_ultima_data,
            v2.gestante_consulta_prenatal_ultima_dias_desde,
            v2.gestante_primeira_dpp,
            v2.atendimento_odontologico_realizado,
            v2.exame_hiv_realizado,
            v2.exame_sifilis_realizado,
            v2.possui_registro_aborto,
            v2.possui_registro_parto
/* v1 */
           FROM ( SELECT v1.atendimento_data,
                    v1.estabelecimento_cnes,
                    v1.estabelecimento_nome,
                    v1.equipe_ine,
                    v1.equipe_nome,
                    v1.acs_nome,
                    v1.acs_data_ultima_visita,
                    v1.co_fat_cidadao_pec,
                    v1.gestante_documento_cpf,
                    v1.gestante_documento_cns,
                    v1.gestante_nome,
                    v1.gestante_telefone,
                    v1.gestante_endereco,
                    v1.gestante_data_de_nascimento,
                    v1.gestante_data_nascimento_ts,
 /* Conta quantas consultas teve a partir da contagem de datas de registro de atendimento ficha de atendimento individual.
São contabilizadas todas as fichas que: 
 - Número de CBO em tb_dim_cbo esta nessa [lista](https://impulsogov.notion.site/Lista-de-tdcbo-nu_cbo-0b7036debf9d48a5a0bf17c1a5c20c89) E número de CIAP em tb_dim_ciap esta nessa [lista](https://impulsogov.notion.site/Lista-de-tdciap-nu_ciap-f0e9d7ad510e4d09b8cc927fba2681b4) OU 
Número de CID nessa [lista](https://impulsogov.notion.site/Lista-de-tdcid-nu_cid-48e1093c19e4473983812d7bd283b34a).
 - Data de registro em tb_dim_tempo é maior que últimos 294 dias*/
                    count(*) OVER (PARTITION BY v1.gestante_nome, v1.gestante_data_de_nascimento) AS gestante_consulta_prenatal_total,
 /* Retorna a data máxima (mais recente) de atendimento a partir do campo registro de atendimento ficha de atendimento individual.
São contabilizadas todas as fichas que: 
 - Número de CBO em tb_dim_cbo esta nessa [lista](https://impulsogov.notion.site/Lista-de-tdcbo-nu_cbo-0b7036debf9d48a5a0bf17c1a5c20c89) E número de CIAP em tb_dim_ciap esta nessa [lista](https://impulsogov.notion.site/Lista-de-tdciap-nu_ciap-f0e9d7ad510e4d09b8cc927fba2681b4) OU 
Número de CID nessa [lista](https://impulsogov.notion.site/Lista-de-tdcid-nu_cid-48e1093c19e4473983812d7bd283b34a).
 - Data de registro em tb_dim_tempo é maior que últimos 294 dias*/
                    max(v1.atendimento_data) OVER (PARTITION BY v1.gestante_nome, v1.gestante_data_de_nascimento) AS gestante_consulta_prenatal_ultima_data,
         /* Retorna diferença entre data atual e data máxima (mais recente) do atendimento, calculada a partir do campo registro de atendimento ficha de atendimento individual.
São contabilizadas todas as fichas que: 
 - Número de CBO em tb_dim_cbo esta nessa [lista](https://impulsogov.notion.site/Lista-de-tdcbo-nu_cbo-0b7036debf9d48a5a0bf17c1a5c20c89) E número de CIAP em tb_dim_ciap esta nessa [lista](https://impulsogov.notion.site/Lista-de-tdciap-nu_ciap-f0e9d7ad510e4d09b8cc927fba2681b4) OU 
Número de CID nessa [lista](https://impulsogov.notion.site/Lista-de-tdcid-nu_cid-48e1093c19e4473983812d7bd283b34a).
 - Data de registro em tb_dim_tempo é maior que últimos 294 dias*/
					CURRENT_DATE - max(v1.atendimento_data) OVER (PARTITION BY v1.gestante_nome, v1.gestante_data_de_nascimento) AS gestante_consulta_prenatal_ultima_dias_desde,
                    first_value(v1.gestante_dpp) OVER (PARTITION BY v1.gestante_nome, v1.gestante_data_de_nascimento ORDER BY v1.atendimento_data) AS gestante_primeira_dpp,
										first_value(v1.gestante_idade_gestacional) OVER (PARTITION BY v1.gestante_nome, v1.gestante_data_de_nascimento ORDER BY v1.atendimento_data) AS gestante_idade_gestacional,
										first_value(v1.gestante_idade_gestacional_atendimento) OVER (PARTITION BY v1.gestante_nome, v1.gestante_data_de_nascimento ORDER BY v1.atendimento_data) AS gestante_idade_gestacional_primeiro_atendimento,               
/* Pega a primeira DUM (ordenada pela data de atendimento) a partir do campo co_dim_tempo_dum da ficha de atendimento individual (tb_fat_atendimento_individual), repartido a partir de nome da gestante e data de nascimento */
										first_value(v1.gestante_dum) OVER (PARTITION BY v1.gestante_nome, v1.gestante_data_de_nascimento ORDER BY v1.atendimento_data) AS gestante_dum_primeiro_atendimento,
                    min(v1.atendimento_data) OVER (PARTITION BY v1.gestante_nome, v1.gestante_data_de_nascimento) AS atendimento_primeiro_data,
/*  Avalia se fez atendimento odontológico através das seguintes condições:
Pega gestantes da Ficha de Atendimento Odontológico Individual (tb_fat_atendimento_odonto):
- Cujo Nome da gestante na ficha tb_fat_cidadao_pec igual tb_fat_cidadao_pec (tfcp.no_cidadao unida pela ficha de atendimento individual) E Data de nascimento na ficha tb_fat_cidadao_pec igual a tb_dim_tempo (tempocidadaopec.dt_registro unida pela ficha cidadao pec pelo tempo de nascimento)
- Data de registro maior ou igual data da DUM 
- Data de registro menor ou igual data do DPP
*/
                    ( SELECT count(*) > 0 AS bool
                           FROM nome_backup.tb_fat_atendimento_odonto otfodont
                             JOIN nome_backup.tb_dim_cbo otdcbo ON otdcbo.co_seq_dim_cbo = otfodont.co_dim_cbo_1
                             JOIN nome_backup.tb_dim_tempo otdtempo ON otdtempo.co_seq_dim_tempo = otfodont.co_dim_tempo
                          WHERE (otfodont.co_fat_cidadao_pec IN ( SELECT tfcodonto.co_seq_fat_cidadao_pec
                                   FROM nome_backup.tb_fat_cidadao_pec tfcodonto
                                  WHERE tfcodonto.no_cidadao::text = v1.gestante_nome::text AND tfcodonto.co_dim_tempo_nascimento = v1.gestante_data_nascimento_ts)) AND otdcbo.nu_cbo::text ~~ '2232%'::text AND otdtempo.dt_registro >= v1.gestante_dum AND otdtempo.dt_registro <= v1.gestante_dpp) AS atendimento_odontologico_realizado,
/*  
Avalia se exame de HIV a partir das seguintes condições:
União de Grupo 1 - Pega da Ficha de Procedimento Individual (tb_fat_proced_atend_proced):
- Cujo Nome da gestante na ficha tb_fat_cidadao_pec igual tb_fat_cidadao_pec (tfcp.no_cidadao unida pela ficha de atendimento individual) E Data de nascimento na ficha tb_fat_cidadao_pec igual a tb_dim_tempo (tempocidadaopec.dt_registro unida pela ficha cidadao pec pelo tempo de nascimento)
- Número de CBO nessa [lista](https://impulsogov.notion.site/Lista-HIV-tdcbo-nu_cbo-541cedbfff83485e83d56c98d3ed5d1e)
- Número de Procedimento nessa [lista](https://impulsogov.notion.site/Lista-HIV-tdp-co_proced-18146819f62645e1a337034ca9dac3ee)
- Data de registro maior ou igual data da DUM 
- Data de registro menor ou igual data do DPP

Com Grupo 2 - Pega da Ficha de Atendimento Individual (tb_fat_atd_ind_procedimentos):
- Cujo Nome da gestante na ficha tb_fat_cidadao_pec igual tb_fat_cidadao_pec (tfcp.no_cidadao unida pela ficha de atendimento individual) E Data de nascimento na ficha tb_fat_cidadao_pec igual a tb_dim_tempo (tempocidadaopec.dt_registro unida pela ficha cidadao pec pelo tempo de nascimento)
- Número de CBO nessa [lista](https://impulsogov.notion.site/Lista-HIV-tdcbo-nu_cbo-541cedbfff83485e83d56c98d3ed5d1e)
- Número de Procedimento nessa [lista](https://impulsogov.notion.site/Lista-HIV-tdp-co_proced-18146819f62645e1a337034ca9dac3ee)
- Data de registro maior ou igual data da DUM 
- Data de registro menor ou igual data do DPP
*/
                    ( SELECT count(*) > 0
                           FROM ( SELECT tdp.co_proced
                                   FROM nome_backup.tb_fat_proced_atend_proced tfpap
                                     JOIN nome_backup.tb_dim_procedimento tdp ON tdp.co_seq_dim_procedimento = tfpap.co_dim_procedimento
                                     JOIN nome_backup.tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfpap.co_dim_cbo
                                     JOIN nome_backup.tb_dim_tempo tdtempo ON tdtempo.co_seq_dim_tempo = tfpap.co_dim_tempo
                                  WHERE (tfpap.co_fat_cidadao_pec IN ( SELECT tfcprocedhiv.co_seq_fat_cidadao_pec
   FROM nome_backup.tb_fat_cidadao_pec tfcprocedhiv
  WHERE tfcprocedhiv.no_cidadao::text = v1.gestante_nome::text AND tfcprocedhiv.co_dim_tempo_nascimento = v1.gestante_data_nascimento_ts)) AND (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND tdtempo.dt_registro >= v1.gestante_dum AND tdtempo.dt_registro <= v1.gestante_dpp AND (tdp.co_proced::text = ANY (ARRAY['0202030300'::character varying::text, 'ABEX018'::character varying::text, '0214010058'::character varying::text, '0214010040'::character varying::text, 'ABPG024'::character varying::text]))
                                UNION ALL
                                 SELECT tdp.co_proced
                                   FROM nome_backup.tb_fat_atd_ind_procedimentos tfaip
                                     JOIN nome_backup.tb_dim_procedimento tdp ON tdp.co_seq_dim_procedimento = tfaip.co_dim_procedimento_avaliado
                                     JOIN nome_backup.tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfaip.co_dim_cbo_1
                                     JOIN nome_backup.tb_dim_tempo tdtempo ON tdtempo.co_seq_dim_tempo = tfaip.co_dim_tempo
                                  WHERE (tfaip.co_fat_cidadao_pec IN ( SELECT tfcprocedhiv.co_seq_fat_cidadao_pec
   FROM nome_backup.tb_fat_cidadao_pec tfcprocedhiv
  WHERE tfcprocedhiv.no_cidadao::text = v1.gestante_nome::text AND tfcprocedhiv.co_dim_tempo_nascimento = v1.gestante_data_nascimento_ts)) AND (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND tdtempo.dt_registro >= v1.gestante_dum AND tdtempo.dt_registro <= v1.gestante_dpp AND (tdp.co_proced::text = ANY (ARRAY['0202030300'::character varying::text, 'ABEX018'::character varying::text, '0214010058'::character varying::text, '0214010040'::character varying::text, 'ABPG024'::character varying::text]))) e1) AS exame_hiv_realizado,
/*  
Avalia se exame de Sifilis a partir das seguintes condições:
União de Grupo 1 - Pega da Ficha de Procedimento Individual (tb_fat_proced_atend_proced):
- Cujo Nome da gestante na ficha tb_fat_cidadao_pec igual tb_fat_cidadao_pec (tfcp.no_cidadao unida pela ficha de atendimento individual) E Data de nascimento na ficha tb_fat_cidadao_pec igual a tb_dim_tempo (tempocidadaopec.dt_registro unida pela ficha cidadao pec pelo tempo de nascimento)
- Número de CBO nessa [lista](https://impulsogov.notion.site/Lista-tdcbo-nu_cbo-541cedbfff83485e83d56c98d3ed5d1e)
- Número de Procedimento nessa [lista](https://impulsogov.notion.site/Lista-Sifilis-tdp-co_proced-1fda8bee5ecc49c3a66c7f737d9a91a5)
- Data de registro maior ou igual data da DUM 
- Data de registro menor ou igual data do DPP

Com Grupo 2 - Pega da Ficha de Atendimento Individual (tb_fat_atd_ind_procedimentos):
- Cujo Nome da gestante na ficha tb_fat_cidadao_pec igual tb_fat_cidadao_pec (tfcp.no_cidadao unida pela ficha de atendimento individual) E Data de nascimento na ficha tb_fat_cidadao_pec igual a tb_dim_tempo (tempocidadaopec.dt_registro unida pela ficha cidadao pec pelo tempo de nascimento)
- Número de CBO nessa [lista](https://impulsogov.notion.site/Lista-tdcbo-nu_cbo-541cedbfff83485e83d56c98d3ed5d1e)
- Número de Procedimento nessa [lista](https://impulsogov.notion.site/Lista-Sifilis-tdp-co_proced-1fda8bee5ecc49c3a66c7f737d9a91a5)
- Data de registro maior ou igual data da DUM 
- Data de registro menor ou igual data do DPP
*/
                    ( SELECT count(*) > 0
                           FROM ( SELECT tdp.co_proced
                                   FROM nome_backup.tb_fat_proced_atend_proced tfpap
                                     JOIN nome_backup.tb_dim_procedimento tdp ON tdp.co_seq_dim_procedimento = tfpap.co_dim_procedimento
                                     JOIN nome_backup.tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfpap.co_dim_cbo
                                     JOIN nome_backup.tb_dim_tempo tdtempo ON tdtempo.co_seq_dim_tempo = tfpap.co_dim_tempo
                                  WHERE (tfpap.co_fat_cidadao_pec IN ( SELECT tfcprocedsilfilis.co_seq_fat_cidadao_pec
   FROM nome_backup.tb_fat_cidadao_pec tfcprocedsilfilis
  WHERE tfcprocedsilfilis.no_cidadao::text = v1.gestante_nome::text AND tfcprocedsilfilis.co_dim_tempo_nascimento = v1.gestante_data_nascimento_ts)) AND (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND tdtempo.dt_registro >= v1.gestante_dum AND tdtempo.dt_registro <= v1.gestante_dpp AND (tdp.co_proced::text = ANY (ARRAY['0202031110'::character varying::text, '0202031179'::character varying::text, 'ABEX019'::character varying::text, '0214010074'::character varying::text, '0214010082'::character varying::text, '0202030300'::character varying::text, '0214010040'::character varying::text, '0214010058'::character varying::text, 'ABPG026'::character varying::text]))
                                UNION ALL
                                 SELECT tdp.co_proced
                                   FROM nome_backup.tb_fat_atd_ind_procedimentos tfaip
                                     JOIN nome_backup.tb_dim_procedimento tdp ON tdp.co_seq_dim_procedimento = tfaip.co_dim_procedimento_avaliado
                                     JOIN nome_backup.tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfaip.co_dim_cbo_1
                                     JOIN nome_backup.tb_dim_tempo tdtempo ON tdtempo.co_seq_dim_tempo = tfaip.co_dim_tempo
                                  WHERE (tfaip.co_fat_cidadao_pec IN ( SELECT tfcprocedsilfilis.co_seq_fat_cidadao_pec
   FROM nome_backup.tb_fat_cidadao_pec tfcprocedsilfilis
  WHERE tfcprocedsilfilis.no_cidadao::text = v1.gestante_nome::text AND tfcprocedsilfilis.co_dim_tempo_nascimento = v1.gestante_data_nascimento_ts)) AND (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND tdtempo.dt_registro >= v1.gestante_dum AND tdtempo.dt_registro <= v1.gestante_dpp AND (tdp.co_proced::text = ANY (ARRAY['0202031110'::character varying::text, '0202031179'::character varying::text, 'ABEX019'::character varying::text, '0214010074'::character varying::text, '0214010082'::character varying::text, '0202030300'::character varying::text, '0214010040'::character varying::text, '0214010058'::character varying::text, 'ABPG026'::character varying::text]))) e2) AS exame_sifilis_realizado,
/* Avalia se houve aborto da gestação a partir das seguintes condições:
- Número de CIAPS nessa [lista](https://impulsogov.notion.site/Lista-de-tdciapaborto-nu_ciap-3553024b7829430aacdec7c337cc9436) E Nome da gestante na ficha tb_fat_cidadao_pec igual tb_fat_cidadao_pec (tfcp.no_cidadao unida pela ficha de atendimento individual) E Data de nascimento na ficha tb_fat_cidadao_pec igual a tb_dim_tempo (tempocidadaopec.dt_registro unida pela ficha cidadao pec pelo tempo de nascimento)
OU Número de CID nessa [lista](https://impulsogov.notion.site/Lista-de-tdcidaborto-nu_cid-6584ff5c22764c5894c1db828f4fdba3) E Data de registro maior ou igual data da DUM E Data de registro menor ou igual data do DPP.
Depois retorna 'Sim' ou 'Não' conforme regras acima. */
                    ( SELECT count(*) > 0
                           FROM nome_backup.tb_fat_atendimento_individual tfaiaborto
                             JOIN nome_backup.tb_fat_atd_ind_problemas tfaipaborto ON tfaiaborto.co_seq_fat_atd_ind = tfaipaborto.co_fat_atd_ind
                             JOIN nome_backup.tb_dim_tempo tdtempoaborto ON tdtempoaborto.co_seq_dim_tempo = tfaiaborto.co_dim_tempo
                             LEFT JOIN nome_backup.tb_dim_cid tdcidaborto ON tdcidaborto.co_seq_dim_cid = tfaipaborto.co_dim_cid
                             LEFT JOIN nome_backup.tb_dim_ciap tdciapaborto ON tdciapaborto.co_seq_dim_ciap = tfaipaborto.co_dim_ciap
                          WHERE (tfaiaborto.co_fat_cidadao_pec IN ( SELECT tfparto.co_seq_fat_cidadao_pec
                                   FROM nome_backup.tb_fat_cidadao_pec tfparto
                                  WHERE tfparto.no_cidadao::text = v1.gestante_nome::text AND tfparto.co_dim_tempo_nascimento = v1.gestante_data_nascimento_ts)) AND ((tdciapaborto.nu_ciap::text = ANY (ARRAY['W82'::character varying::text, 'W83'::character varying::text])) OR (tdcidaborto.nu_cid::text = ANY (ARRAY['O02'::character varying::text, 'O03'::character varying::text, 'O05'::character varying::text, 'O06'::character varying::text, 'O04'::character varying::text, 'Z303'::character varying::text]))) AND tdtempoaborto.dt_registro >= v1.gestante_dum AND tdtempoaborto.dt_registro <= v1.gestante_dpp) AS possui_registro_aborto,

/* Avalia se houve parto da gestante a partir das seguintes condições:
- Número de CIAPS nessa [lista](https://impulsogov.notion.site/Lista-de-tdciapparto-nu_ciap-13968ef64119490aaf36ace6ecae9a06) E Nome da gestante na ficha tb_fat_cidadao_pec igual tb_fat_cidadao_pec (tfcp.no_cidadao unida pela ficha de atendimento individual) E Data de nascimento na ficha tb_fat_cidadao_pec igual a tb_dim_tempo (tempocidadaopec.dt_registro unida pela ficha cidadao pec pelo tempo de nascimento)
OU Número de CID nessa [lista](https://impulsogov.notion.site/Lista-de-tdcidparto-nu_cid-cde4796ac50a4858a8077546190fa03f) E Data de registro maior ou igual data da DUM E Data de registro menor ou igual data do DPP.
Depois retorna 'Sim' ou 'Não' conforme regras acima. */
                    ( SELECT count(*) > 0
                           FROM nome_backup.tb_fat_atendimento_individual tfaiparto
                             JOIN nome_backup.tb_fat_atd_ind_problemas tfaipparto ON tfaiparto.co_seq_fat_atd_ind = tfaipparto.co_fat_atd_ind
                             JOIN nome_backup.tb_dim_tempo tdtempoparto ON tdtempoparto.co_seq_dim_tempo = tfaiparto.co_dim_tempo
                             LEFT JOIN nome_backup.tb_dim_cid tdcidparto ON tdcidparto.co_seq_dim_cid = tfaipparto.co_dim_cid
                             LEFT JOIN nome_backup.tb_dim_ciap tdciapparto ON tdciapparto.co_seq_dim_ciap = tfaipparto.co_dim_ciap
                          WHERE (tfaiparto.co_fat_cidadao_pec IN ( SELECT tfcparto.co_seq_fat_cidadao_pec
                                   FROM nome_backup.tb_fat_cidadao_pec tfcparto
                                  WHERE tfcparto.no_cidadao::text = v1.gestante_nome::text AND tfcparto.co_dim_tempo_nascimento = v1.gestante_data_nascimento_ts)) AND ((tdciapparto.nu_ciap::text = ANY (ARRAY['W90'::character varying::text, 'W91'::character varying::text, 'W92'::character varying::text, 'W93'::character varying::text])) OR (tdcidparto.nu_cid::text = ANY (ARRAY['O80'::character varying::text, 'Z370'::character varying::text, 'Z379'::character varying::text, 'Z38'::character varying::text, 'Z39'::character varying::text, 'Z371'::character varying::text, 'Z379'::character varying::text, 'O42'::character varying::text, 'O45'::character varying::text, 'O60'::character varying::text, 'O61'::character varying::text, 'O62'::character varying::text, 'O63'::character varying::text, 'O64'::character varying::text, 'O65'::character varying::text, 'O66'::character varying::text, 'O67'::character varying::text, 'O68'::character varying::text, 'O69'::character varying::text, 'O70'::character varying::text, 'O71'::character varying::text, 'O73'::character varying::text, 'O750'::character varying::text, 'O751'::character varying::text, 'O754'::character varying::text, 'O755'::character varying::text, 'O756'::character varying::text, 'O757'::character varying::text, 'O758'::character varying::text, 'O759'::character varying::text, 'O81'::character varying::text, 'O82'::character varying::text, 'O83'::character varying::text, 'O84'::character varying::text, 'Z372'::character varying::text, 'Z375'::character varying::text, 'Z379'::character varying::text, 'Z38'::character varying::text, 'Z39'::character varying::text]))) AND tdtempoparto.dt_registro >= v1.gestante_dum AND tdtempoparto.dt_registro <= v1.gestante_dpp) AS possui_registro_parto
								FROM ( SELECT tdt.dt_registro AS atendimento_data,
/* Retorna código da unidade na Ficha de cadastro individual recente (tb_fat_cad_individual), caso nulo retorna código da unidade na Ficha de atendimento individual recente (tb_fat_atendimento_individual) */
                            COALESCE(NULLIF(unidadecadastrorecente.nu_cnes::text, '-'::text), unidadeatendimentorecente.nu_cnes::text) AS estabelecimento_cnes,
/* Retorna nome da unidade na Ficha de cadastro individual recente (tb_fat_cad_individual), caso nulo retorna nome da unidade na Ficha de atendimento individual recente (tb_fat_atendimento_individual) */
                            COALESCE(NULLIF(unidadecadastrorecente.no_unidade_saude::text, 'Não informado'::text), unidadeatendimentorecente.no_unidade_saude::text) AS estabelecimento_nome,
/* Retorna código da equipe na Ficha de cadastro individual recente (tb_fat_cad_individual), caso nulo retorna código da equipe na Ficha de atendimento individual recente (tb_fat_atendimento_individual) */
                            COALESCE(NULLIF(equipeacadastrorecente.nu_ine::text, '-'::text), equipeatendimentorecente.nu_ine::text) AS equipe_ine,
/* Retorna nome da equipe na Ficha de cadastro individual recente (tb_fat_cad_individual), caso nulo retorna nome da equipe na Ficha de atendimento individual recente (tb_fat_atendimento_individual) */
                            COALESCE(NULLIF(equipeacadastrorecente.no_equipe::text, 'SEM EQUIPE'::text), equipeatendimentorecente.no_equipe::text) AS equipe_nome,
/* Retorna nome do ACS na Ficha de cadastro individual recente (tb_fat_cad_individual), caso nulo retorna nome do ACS da Ficha de Visita Domiciliar (tb_fat_visita_domiciliar) */
                            COALESCE(acsvisitarecente.no_profissional, acscadastrorecente.no_profissional) AS acs_nome,
/* Retorna a data de última visita do ACS, a partir da Ficha de Visita Domiciliar (tb_fat_visita_domiciliar)  */
                            acstempovisitarecente.dt_registro AS acs_data_ultima_visita,
                            tfai.co_fat_cidadao_pec,
/* Retorna o CPF Ficha do Cidadao PEC (tb_fat_cidadao_pec)*/
                            tfcp.nu_cpf_cidadao AS gestante_documento_cpf,
/* Retorna o CNS Ficha do Cidadao PEC (tb_fat_cidadao_pec)*/
                            tfcp.nu_cns AS gestante_documento_cns,
/* Retorna o Nome da Ficha do Cidadao PEC (tb_fat_cidadao_pec)*/
                            tfcp.no_cidadao AS gestante_nome,
/* Retorna o número de telefone Ficha do Cidadao PEC (tb_fat_cidadao_pec)*/
                            tfcp.nu_telefone_celular AS gestante_telefone,
/* Retorna o endereço Ficha de Cadastro Domiciliar (tb_fat_cad_domiciliar)  */
                            NULLIF(concat(tfcd.no_logradouro, ', ', tfcd.nu_num_logradouro), ', '::text) AS gestante_endereco,
/* Retorna a data de nascimento da tabela tb_dim_tempo vinculado com a Ficha de Cadastro Domiciliar (tb_fat_cad_domiciliar) pelo campo co_dim_tempo_nascimento */
														tempocidadaopec.dt_registro AS gestante_data_de_nascimento,
/* Nascimento da Ficha de Cadastro Domiciliar (tb_fat_cad_domiciliar) usada para filtrar exames mas não como data mostrada */
                            tfcp.co_dim_tempo_nascimento AS gestante_data_nascimento_ts,
/* 
Calcula a DPP por:
- Caso data de registro da Ficha de Atendimento Individual (tb_fat_atendimento_individual) preenchida corretamente (diferente de 3000), retorna data de registro + 294 dias
- Caso idade gestacional (nu_idade_gestacional_semanas) da Ficha de Atendimento Individual (tb_fat_atendimento_individual) preenchida, retorna data de registro - 7 dias * Idade gestacional em semanas + 294 dias
- Caso ambos esteja vazio, fica nulo
Pega a primeira DPP (ordenada pela data de atendimento), repartido a partir de nome da gestante e data de nascimento */
                                CASE
                                    WHEN tdtdum.nu_ano <> 3000 THEN tdtdum.dt_registro + '294 days'::interval
                                    WHEN tfai.nu_idade_gestacional_semanas IS NOT NULL THEN tdt.dt_registro - '7 days'::interval * tfai.nu_idade_gestacional_semanas::double precision + '294 days'::interval
                                    ELSE NULL::timestamp without time zone
                                END AS gestante_dpp,
/* 
Calcula a Idade Gestacional por:
- Caso data de registro da Ficha de Atendimento Individual (tb_fat_atendimento_individual) preenchida corretamente (diferente de 3000), retorna (data atual - data de registro)/7
- Caso idade gestacional (nu_idade_gestacional_semanas) da Ficha de Atendimento Individual (tb_fat_atendimento_individual) preenchida, retorna ((data atual - (data de registro - 7 dias * Idade gestacional em semanas )) / 7)
- Caso ambos esteja vazio, fica nulo
Pega a primeira ordenada pela data de atendimento, repartido a partir de nome da gestante e data de nascimento */
                                CASE
                                    WHEN tdtdum.nu_ano <> 3000 THEN (CURRENT_DATE - tdtdum.dt_registro) / 7
                                    WHEN tfai.nu_idade_gestacional_semanas IS NOT NULL THEN (CURRENT_DATE - (tdt.dt_registro - '7 days'::interval * tfai.nu_idade_gestacional_semanas::double precision)::date) / 7
                                    ELSE NULL::integer
                                END AS gestante_idade_gestacional,
                            tfai.nu_idade_gestacional_semanas AS gestante_idade_gestacional_atendimento,
                            tdtdum.dt_registro AS gestante_dum
/* Inicio do FROM */
														 FROM nome_backup.tb_fat_atendimento_individual tfai
                             JOIN nome_backup.tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfai.co_dim_cbo_1
                             JOIN nome_backup.tb_dim_tempo tdt ON tfai.co_dim_tempo = tdt.co_seq_dim_tempo
                             JOIN nome_backup.tb_dim_tempo tdtdum ON tfai.co_dim_tempo_dum = tdtdum.co_seq_dim_tempo
                             JOIN nome_backup.tb_fat_atd_ind_problemas tfaip ON tfai.co_seq_fat_atd_ind = tfaip.co_fat_atd_ind
                             JOIN nome_backup.tb_fat_cidadao_pec tfcp ON tfcp.co_seq_fat_cidadao_pec = tfai.co_fat_cidadao_pec
                             JOIN nome_backup.tb_dim_tempo tempocidadaopec ON tempocidadaopec.co_seq_dim_tempo = tfcp.co_dim_tempo_nascimento
                             LEFT JOIN nome_backup.tb_dim_cid tdcid ON tdcid.co_seq_dim_cid = tfaip.co_dim_cid
                             LEFT JOIN nome_backup.tb_dim_ciap tdciap ON tdciap.co_seq_dim_ciap = tfaip.co_dim_ciap
                             LEFT JOIN nome_backup.tb_cidadao cidadao ON tfai.nu_cns = cidadao.nu_cns::bpchar OR tfai.nu_cpf_cidadao::text = cidadao.nu_cpf::text
                             LEFT JOIN nome_backup.tb_fat_cad_domiciliar tfcd ON tfcd.co_seq_fat_cad_domiciliar = (( SELECT cadomiciliar.co_seq_fat_cad_domiciliar
                                   FROM nome_backup.tb_fat_cad_dom_familia caddomiciliarfamilia
                                     JOIN nome_backup.tb_fat_cad_domiciliar cadomiciliar ON cadomiciliar.co_seq_fat_cad_domiciliar = caddomiciliarfamilia.co_fat_cad_domiciliar
                                  WHERE (caddomiciliarfamilia.co_fat_cidadao_pec IN ( SELECT tfccaddomiciliarfamilia.co_seq_fat_cidadao_pec
   FROM nome_backup.tb_fat_cidadao_pec tfccaddomiciliarfamilia
  WHERE tfccaddomiciliarfamilia.no_cidadao::text = tfcp.no_cidadao::text AND tfccaddomiciliarfamilia.co_dim_tempo_nascimento = tfcp.co_dim_tempo_nascimento))
                                  ORDER BY cadomiciliar.co_dim_tempo DESC
                                 LIMIT 1))
                             LEFT JOIN nome_backup.tb_fat_visita_domiciliar tfvdrecente ON tfvdrecente.co_seq_fat_visita_domiciliar = (( SELECT visitadomiciliar.co_seq_fat_visita_domiciliar
                                   FROM nome_backup.tb_fat_visita_domiciliar visitadomiciliar
                                  WHERE (visitadomiciliar.co_fat_cidadao_pec IN ( SELECT tfcvisita.co_seq_fat_cidadao_pec
   FROM nome_backup.tb_fat_cidadao_pec tfcvisita
  WHERE tfcvisita.no_cidadao::text = tfcp.no_cidadao::text AND tfcvisita.co_dim_tempo_nascimento = tfcp.co_dim_tempo_nascimento))
                                  ORDER BY visitadomiciliar.co_dim_tempo DESC
                                 LIMIT 1))
                             LEFT JOIN nome_backup.tb_fat_cad_individual tfcirecente ON tfcirecente.co_seq_fat_cad_individual = (( SELECT cadastroindividual.co_seq_fat_cad_individual
                                   FROM nome_backup.tb_fat_cad_individual cadastroindividual
                                  WHERE (cadastroindividual.co_fat_cidadao_pec IN ( SELECT tfccadastro.co_seq_fat_cidadao_pec
   FROM nome_backup.tb_fat_cidadao_pec tfccadastro
  WHERE tfccadastro.no_cidadao::text = tfcp.no_cidadao::text AND tfccadastro.co_dim_tempo_nascimento = tfcp.co_dim_tempo_nascimento))
                                  ORDER BY cadastroindividual.co_dim_tempo DESC
                                 LIMIT 1))
                             LEFT JOIN nome_backup.tb_fat_atendimento_individual tfairecente ON tfairecente.co_seq_fat_atd_ind = (( SELECT atendimentoindividual.co_seq_fat_atd_ind
                                   FROM nome_backup.tb_fat_atendimento_individual atendimentoindividual
                                  WHERE (atendimentoindividual.co_fat_cidadao_pec IN ( SELECT tfcatendimento.co_seq_fat_cidadao_pec
   FROM nome_backup.tb_fat_cidadao_pec tfcatendimento
  WHERE tfcatendimento.no_cidadao::text = tfcp.no_cidadao::text AND tfcatendimento.co_dim_tempo_nascimento = tfcp.co_dim_tempo_nascimento))
                                  ORDER BY atendimentoindividual.co_dim_tempo DESC
                                 LIMIT 1))
                             LEFT JOIN nome_backup.tb_dim_equipe equipeatendimentorecente ON equipeatendimentorecente.co_seq_dim_equipe = tfairecente.co_dim_equipe_1
                             LEFT JOIN nome_backup.tb_dim_profissional profissinalatendimentorecente ON profissinalatendimentorecente.co_seq_dim_profissional = tfairecente.co_dim_profissional_1
                             LEFT JOIN nome_backup.tb_dim_unidade_saude unidadeatendimentorecente ON unidadeatendimentorecente.co_seq_dim_unidade_saude = tfairecente.co_dim_unidade_saude_1
                             LEFT JOIN nome_backup.tb_dim_equipe equipeacadastrorecente ON equipeacadastrorecente.co_seq_dim_equipe = tfcirecente.co_dim_equipe
                             LEFT JOIN nome_backup.tb_dim_profissional profissinalcadastrorecente ON profissinalcadastrorecente.co_seq_dim_profissional = tfcirecente.co_dim_profissional
                             LEFT JOIN nome_backup.tb_dim_unidade_saude unidadecadastrorecente ON unidadecadastrorecente.co_seq_dim_unidade_saude = tfcirecente.co_dim_unidade_saude
                             LEFT JOIN nome_backup.tb_dim_profissional acsvisitarecente ON acsvisitarecente.co_seq_dim_profissional = tfvdrecente.co_dim_profissional
                             LEFT JOIN nome_backup.tb_dim_profissional acscadastrorecente ON acscadastrorecente.co_seq_dim_profissional = tfcirecente.co_dim_profissional
                             LEFT JOIN nome_backup.tb_dim_tempo acstempovisitarecente ON tfvdrecente.co_dim_tempo = acstempovisitarecente.co_seq_dim_tempo
/* Filtra os registros que tenham as seguintes condições: 
 - Número de CBO em tb_dim_cbo esta nessa [lista](https://impulsogov.notion.site/Lista-de-tdcbo-nu_cbo-0b7036debf9d48a5a0bf17c1a5c20c89) E número de CIAP em tb_dim_ciap esta nessa [lista](https://impulsogov.notion.site/Lista-de-tdciap-nu_ciap-f0e9d7ad510e4d09b8cc927fba2681b4) OU 
Número de CID nessa [lista](https://impulsogov.notion.site/Lista-de-tdcid-nu_cid-48e1093c19e4473983812d7bd283b34a).
 - Data de registro em tb_dim_tempo é maior que últimos 294 dias. */
                          WHERE (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2231%'::text, '2235%'::text, '2251%'::text, '2252%'::text, '2253%'::text])) AND ((tdciap.nu_ciap::text = ANY (ARRAY['ABP001'::character varying::text, 'W03'::character varying::text, 'W05'::character varying::text, 'W29'::character varying::text, 'W71'::character varying::text, 'W78'::character varying::text, 'W79'::character varying::text, 'W80'::character varying::text, 'W81'::character varying::text, 'W84'::character varying::text, 'W85'::character varying::text])) OR (tdcid.nu_cid::text = ANY (ARRAY['O11'::character varying::text, 'O120'::character varying::text, 'O121'::character varying::text, 'O122'::character varying::text, 'O13'::character varying::text, 'O140'::character varying::text, 'O141'::character varying::text, 'O149'::character varying::text, 'O150'::character varying::text, 'O151'::character varying::text, 'O159'::character varying::text, 'O16'::character varying::text, 'O200'::character varying::text, 'O208'::character varying::text, 'O209'::character varying::text, 'O210'::character varying::text, 'O211'::character varying::text, 'O212'::character varying::text, 'O218'::character varying::text, 'O219'::character varying::text, 'O220'::character varying::text, 'O221'::character varying::text, 'O222'::character varying::text, 'O223'::character varying::text, 'O224'::character varying::text, 'O225'::character varying::text, 'O228'::character varying::text, 'O229'::character varying::text, 'O230'::character varying::text, 'O231'::character varying::text, 'O232'::character varying::text, 'O233'::character varying::text, 'O234'::character varying::text, 'O235'::character varying::text, 'O239'::character varying::text, 'O299'::character varying::text, 'O300'::character varying::text, 'O301'::character varying::text, 'O302'::character varying::text, 'O308'::character varying::text, 'O309'::character varying::text, 'O311'::character varying::text, 'O312'::character varying::text, 'O318'::character varying::text, 'O320'::character varying::text, 'O321'::character varying::text, 'O322'::character varying::text, 'O323'::character varying::text, 'O324'::character varying::text, 'O325'::character varying::text, 'O326'::character varying::text, 'O328'::character varying::text, 'O329'::character varying::text, 'O330'::character varying::text, 'O331'::character varying::text, 'O332'::character varying::text, 'O333'::character varying::text, 'O334'::character varying::text, 'O335'::character varying::text, 'O336'::character varying::text, 'O337'::character varying::text, 'O338'::character varying::text, 'O752'::character varying::text, 'O753'::character varying::text, 'O990'::character varying::text, 'O991'::character varying::text, 'O992'::character varying::text, 'O993'::character varying::text, 'O994'::character varying::text, 'O240'::character varying::text, 'O241'::character varying::text, 'O242'::character varying::text, 'O243'::character varying::text, 'O244'::character varying::text, 'O249'::character varying::text, 'O25'::character varying::text, 'O260'::character varying::text, 'O261'::character varying::text, 'O263'::character varying::text, 'O264'::character varying::text, 'O265'::character varying::text, 'O268'::character varying::text, 'O269'::character varying::text, 'O280'::character varying::text, 'O281'::character varying::text, 'O282'::character varying::text, 'O283'::character varying::text, 'O284'::character varying::text, 'O285'::character varying::text, 'O288'::character varying::text, 'O289'::character varying::text, 'O290'::character varying::text, 'O291'::character varying::text, 'O292'::character varying::text, 'O293'::character varying::text, 'O294'::character varying::text, 'O295'::character varying::text, 'O296'::character varying::text, 'O298'::character varying::text, 'O009'::character varying::text, 'O339'::character varying::text, 'O340'::character varying::text, 'O341'::character varying::text, 'O342'::character varying::text, 'O343'::character varying::text, 'O344'::character varying::text, 'O345'::character varying::text, 'O346'::character varying::text, 'O347'::character varying::text, 'O348'::character varying::text, 'O349'::character varying::text, 'O350'::character varying::text, 'O351'::character varying::text, 'O352'::character varying::text, 'O353'::character varying::text, 'O354'::character varying::text, 'O355'::character varying::text, 'O356'::character varying::text, 'O357'::character varying::text, 'O358'::character varying::text, 'O359'::character varying::text, 'O360'::character varying::text, 'O361'::character varying::text, 'O362'::character varying::text, 'O363'::character varying::text, 'O365'::character varying::text, 'O366'::character varying::text, 'O367'::character varying::text, 'O368'::character varying::text, 'O369'::character varying::text, 'O40'::character varying::text, 'O410'::character varying::text, 'O411'::character varying::text, 'O418'::character varying::text, 'O419'::character varying::text, 'O430'::character varying::text, 'O431'::character varying::text, 'O438'::character varying::text, 'O439'::character varying::text, 'O440'::character varying::text, 'O441'::character varying::text, 'O460'::character varying::text, 'O468'::character varying::text, 'O469'::character varying::text, 'O470'::character varying::text, 'O471'::character varying::text, 'O479'::character varying::text, 'O48'::character varying::text, 'O995'::character varying::text, 'O996'::character varying::text, 'O997'::character varying::text, 'Z640'::character varying::text, 'O00'::character varying::text, 'O10'::character varying::text, 'O12'::character varying::text, 'O14'::character varying::text, 'O15'::character varying::text, 'O20'::character varying::text, 'O21'::character varying::text, 'O22'::character varying::text, 'O23'::character varying::text, 'O24'::character varying::text, 'O26'::character varying::text, 'O28'::character varying::text, 'O29'::character varying::text, 'O30'::character varying::text, 'O31'::character varying::text, 'O32'::character varying::text, 'O33'::character varying::text, 'O34'::character varying::text, 'O35'::character varying::text, 'O36'::character varying::text, 'O41'::character varying::text, 'O43'::character varying::text, 'O44'::character varying::text, 'O46'::character varying::text, 'O47'::character varying::text, 'O98'::character varying::text, 'Z34'::character varying::text, 'Z35'::character varying::text, 'Z36'::character varying::text, 'Z321'::character varying::text, 'Z33'::character varying::text, 'Z340'::character varying::text, 'Z348'::character varying::text, 'Z349'::character varying::text, 'Z350'::character varying::text, 'Z351'::character varying::text, 'Z352'::character varying::text, 'Z353'::character varying::text, 'Z354'::character varying::text, 'Z357'::character varying::text, 'Z358'::character varying::text, 'Z359'::character varying::text]))) AND tdt.dt_registro >= (CURRENT_DATE - '294 days'::interval)) v1) v2) v3
/* Filtra:
 - Primeira linha para cada gestante
 - Cuja data provável de parto maior que a data atual */
  WHERE v3.r = 1 AND v3.gestante_primeira_dpp >= CURRENT_DATE
)
WITH DATA;
