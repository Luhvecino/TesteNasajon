function normalizar(texto) {
    return texto
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase()
}

function levenshtein(a, b) {

    const matrix = []

    for (let i = 0; i <= b.length; i++) {
        matrix[i] = [i]
    }

    for (let j = 0; j <= a.length; j++) {
        matrix[0][j] = j
    }

    for (let i = 1; i <= b.length; i++) {
        for (let j = 1; j <= a.length; j++) {

            if (b.charAt(i - 1) === a.charAt(j - 1)) {
                matrix[i][j] = matrix[i - 1][j - 1]
            } else {
                matrix[i][j] = Math.min(
                    matrix[i - 1][j - 1] + 1,
                    matrix[i][j - 1] + 1,
                    matrix[i - 1][j] + 1
                )
            }

        }
    }

    return matrix[b.length][a.length]
}

function similaridade(a, b) {

    const distancia = levenshtein(a, b)
    const max = Math.max(a.length, b.length)

    return 1 - distancia / max

}

function encontrarMunicipio(nome, mapaMunicipios) {

    const nomeNormalizado = normalizar(nome)

    let melhorMatch = null
    let melhorScore = 0

    for (const chave in mapaMunicipios) {

        const score = similaridade(nomeNormalizado, chave)

        if (score > melhorScore) {
            melhorScore = score
            melhorMatch = mapaMunicipios[chave]
        }

    }

    if (melhorScore >= 0.8) {
        return melhorMatch
    }

    return null
}

module.exports = {
    normalizar,
    levenshtein,
    similaridade,
    encontrarMunicipio
}