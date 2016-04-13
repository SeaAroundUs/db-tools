USE [Merlin];
with side_by_side as (
				SELECT v40.[TaxonKey]
					  ,v40.[CellID]
					  , v40.RelativeAbundance as V40_RelativeAbundance
					  , v42.RelativeAbundance as V42_RelativeAbundance
						FROM [dbo].[TaxonDistribution_V40] v40 left join [TaxonDistribution] v42 on v40.TaxonKey = v42.TaxonKey and v40.CellID = v42.CellID
				  	),
 zeroed_cells as (
	  select s.TaxonKey,  count(*) as Count_of_cells_PreviouslyNonZeroRelativeAbundance_but_NowZero
	  from side_by_side s 
	  where V42_RelativeAbundance is null
	  and s.TaxonKey in (select distinct TaxonKey from TaxonDistribution)
	  group by s.TaxonKey),
 original_coverage as (
           select taxonkey, count(*) as v40_non_zero_cells
		   from TaxonDistribution_V40
		   where RelativeAbundance > 0	   
		   group by TaxonKey
		),
new_coverage as (
select taxonkey, count(*) as v42_non_zero_cells
		   from TaxonDistribution
		   where RelativeAbundance > 0   
		   group by TaxonKey
		),

final_report as ( 
		select z.TaxonKey,   z.Count_of_cells_PreviouslyNonZeroRelativeAbundance_but_NowZero, o.v40_non_zero_cells
		 from zeroed_cells z inner join original_coverage o
		 		 on z.TaxonKey = o.TaxonKey
				 )

 select t.[Common Name], f.*, (select top 1 v42_non_zero_cells from new_coverage n where n.TaxonKey = f.TaxonKey) as v42_non_zero_cells
 from final_report f inner join Cube_DimTaxon t on f.TaxonKey = t.TaxonKey
 where f.TaxonKey in (select distinct TaxonKey from TaxonDistribution)
 order by Count_of_cells_PreviouslyNonZeroRelativeAbundance_but_NowZero desc

 