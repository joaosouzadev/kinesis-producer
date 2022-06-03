package pay_worker

type Entry struct {
	Type          string `json:"type"`
	Amount        int    `json:"amount"`
	Installment   int    `json:"installment"`
	EffectiveDate string `json:"effective_date"`
}

type Payment struct {
	MerchantId string  `json:"merchant_id"`
	PaymentId  string  `json:"payment_id"`
	Amount     int     `json:"amount"`
	Entry      []Entry `json:"entries"`
}
