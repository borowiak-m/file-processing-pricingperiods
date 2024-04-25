# file-processing-pricingperiods
Golang program that will handle large db extracts and collapse / flatten overlapping pricing period entries based on their priority indicators.

TO DO
- [ ]  add chunker func to split recordset to manageable pieces
- [ ]  add workers func to spawn workper per chunk from chunker
- [ ]  add collector func to get all processed results from workers

Considerations: numbers of cores per machine, how to chunk data to maintain a coherent batch of product/customer combination
