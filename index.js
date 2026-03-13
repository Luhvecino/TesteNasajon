const {
    normalizar,
    encontrarMunicipio
} = require("./utils")
const fs = require("fs")
const csv = require("csv-parser")
const axios = require("axios")
const createCsvWriter = require("csv-writer").createObjectCsvWriter

const PROJECT_FUNCTION_URL =
    "https://mynxlubykylncinttggu.functions.supabase.co/ibge-submit"

const ACCESS_TOKEN = "eyJhbGciOiJIUzI1NiIsImtpZCI6ImR0TG03UVh1SkZPVDJwZEciLCJ0eXAiOiJKV1QifQ.eyJpc3MiOiJodHRwczovL215bnhsdWJ5a3lsbmNpbnR0Z2d1LnN1cGFiYXNlLmNvL2F1dGgvdjEiLCJzdWIiOiI5MjI0YzYzYi0wNWY5LTRiNmMtYTYxMC04NmNjNzA2ZWNmYzgiLCJhdWQiOiJhdXRoZW50aWNhdGVkIiwiZXhwIjoxNzczNDEzMDk3LCJpYXQiOjE3NzM0MDk0OTcsImVtYWlsIjoibHVjYXN2ZWNpbm9AZ21haWwuY29tIiwicGhvbmUiOiIiLCJhcHBfbWV0YWRhdGEiOnsicHJvdmlkZXIiOiJlbWFpbCIsInByb3ZpZGVycyI6WyJlbWFpbCJdfSwidXNlcl9tZXRhZGF0YSI6eyJlbWFpbCI6Imx1Y2FzdmVjaW5vQGdtYWlsLmNvbSIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJub21lIjoiTHVjYXMgVmVjaW5vIFJvZHJpZ3VlcyIsInBob25lX3ZlcmlmaWVkIjpmYWxzZSwic3ViIjoiOTIyNGM2M2ItMDVmOS00YjZjLWE2MTAtODZjYzcwNmVjZmM4In0sInJvbGUiOiJhdXRoZW50aWNhdGVkIiwiYWFsIjoiYWFsMSIsImFtciI6W3sibWV0aG9kIjoicGFzc3dvcmQiLCJ0aW1lc3RhbXAiOjE3NzM0MDk0OTd9XSwic2Vzc2lvbl9pZCI6Ijg4NjRmZjk5LWQ0YTItNDNjMS1hYWI3LTdmZTU0NTc4ZDFiOCIsImlzX2Fub255bW91cyI6ZmFsc2V9.oUvOntFuFn0rK-R7nNVREvrdk16KdswLUWuvEDGyseA"


const municipios = []
const municipiosUnicos = new Set()

const correcoes = {
    "Belo Horzionte": "Belo Horizonte",
    "Curitba": "Curitiba",
    "Santoo Andre": "Santos Andre",
    "Santo Andre": "Santos Andre"
}

function corrigirMunicipio(nome) {

    if (correcoes[nome]) {
        return correcoes[nome]
    }

    return nome
}

fs.createReadStream("input.csv")
    .pipe(csv())
    .on("data", (row) => {
        const municipioCorrigido = corrigirMunicipio(row.municipio)

        if (!municipiosUnicos.has(municipioCorrigido)) {

            municipiosUnicos.add(municipioCorrigido)

            municipios.push({
                municipio: municipioCorrigido,
                populacao: row.populacao
            })

        }
    })
    .on("end", async () => {
        const municipiosIBGEResponse = await axios.get(
            "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
        )

        const mapaMunicipios = {}

        for (const m of municipiosIBGEResponse.data) {

            mapaMunicipios[normalizar(m.nome)] = {
                id: m.id,
                nome: m.nome,
                uf: m.microrregiao?.mesorregiao?.UF?.sigla || "",
                regiao: m.microrregiao?.mesorregiao?.UF?.regiao?.nome || ""
            }

        }

        console.log("Mapa de municípios carregado:", Object.keys(mapaMunicipios).length)


        const resultado = []

        for (const cidade of municipios) {

            const dadosIBGE = encontrarMunicipio(cidade.municipio, mapaMunicipios)

            if (dadosIBGE) {

                resultado.push({
                    municipio_input: cidade.municipio,
                    populacao_input: cidade.populacao,
                    municipio_ibge: dadosIBGE.nome,
                    uf: dadosIBGE.uf,
                    regiao: dadosIBGE.regiao,
                    id_ibge: dadosIBGE.id,
                    status: "OK"
                })

            } else {

                resultado.push({
                    municipio_input: cidade.municipio,
                    populacao_input: cidade.populacao,
                    municipio_ibge: "",
                    uf: "",
                    regiao: "",
                    id_ibge: "",
                    status: "NAO_ENCONTRADO"
                })

            }

        }

        const writer = createCsvWriter({
            path: "resultado.csv",
            header: [
                { id: "municipio_input", title: "municipio_input" },
                { id: "populacao_input", title: "populacao_input" },
                { id: "municipio_ibge", title: "municipio_ibge" },
                { id: "uf", title: "uf" },
                { id: "regiao", title: "regiao" },
                { id: "id_ibge", title: "id_ibge" },
                { id: "status", title: "status" }
            ]
        })

        await writer.writeRecords(resultado)

        console.log("CSV gerado!")

        const total_municipios = resultado.length
        const total_ok = resultado.filter(r => r.status === "OK").length
        const total_nao_encontrado = resultado.filter(r => r.status === "NAO_ENCONTRADO").length
        const total_erro_api = resultado.filter(r => r.status === "ERRO_API").length
        const pop_total_ok = resultado
            .filter(r => r.status === "OK")
            .reduce((acc, r) => acc + Number(r.populacao_input), 0)
        const medias_por_regiao = {}
        const regioes = {}

        for (const r of resultado) {

            if (r.status !== "OK") continue

            if (!regioes[r.regiao]) {
                regioes[r.regiao] = {
                    soma: 0,
                    count: 0
                }
            }

            regioes[r.regiao].soma += Number(r.populacao_input)
            regioes[r.regiao].count++

        }

        for (const regiao in regioes) {

            medias_por_regiao[regiao] =
                regioes[regiao].soma / regioes[regiao].count

        }

        const estatisticas = {
            total_municipios,
            total_ok,
            total_nao_encontrado,
            total_erro_api,
            pop_total_ok,
            medias_por_regiao
        }

        console.log(estatisticas)

        const response = await axios.post(
            PROJECT_FUNCTION_URL,
            { stats: estatisticas },
            {
                headers: {
                    Authorization: `Bearer ${ACCESS_TOKEN}`,
                    "Content-Type": "application/json"
                }
            }
        )

        console.log("Resposta da API:", response.data)

    })