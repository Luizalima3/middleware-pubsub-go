package broker

// Códigos de erro enviados no Conteudo de um TipoErro, como texto UTF-8 exato.
//
// O código RATE_LIMIT não aparece aqui: a resposta de limite de taxa é
// produzida pelo pacote controle (Bucket.Verificar) e repassada sem alteração.
const (
	// CodigoTopicoInexistente: Publicar ou Assinar em tópico que não existe.
	CodigoTopicoInexistente = "TOPICO_INEXISTENTE"
	// CodigoTopicoInvalido: CriarTopico com nome vazio.
	CodigoTopicoInvalido = "TOPICO_INVALIDO"
	// CodigoTopicoSobrecarregado: a fila do tópico está cheia; a publicação foi descartada.
	CodigoTopicoSobrecarregado = "TOPICO_SOBRECARREGADO"
	// CodigoTopicoApagado: aviso espontâneo (ID 0) aos inscritos de um tópico apagado.
	CodigoTopicoApagado = "TOPICO_APAGADO"
	// CodigoFrameInvalido: frame de texto ou que não decodifica (ID 0). A conexão continua.
	CodigoFrameInvalido = "FRAME_INVALIDO"
	// CodigoTipoDesconhecido: tipo de broker→cliente (16–20) ou desconhecido vindo do cliente.
	CodigoTipoDesconhecido = "TIPO_DESCONHECIDO"
)
