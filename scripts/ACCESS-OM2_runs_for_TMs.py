import intake
catalog = intake.cat.access_nri

variable=[
    "tx_trans", "ty_trans",
    "mld",
    "area_t",
    "^d[hz]t$",
]

subcat = catalog.search(
    model="ACCESS-OM2.*",
    variable=variable,
    require_all=True
)

df = subcat.df

print(df[df.columns[:-1]].to_markdown())

df.to_html('output/ACCESS-OM2_runs_for_TMs.html')

df[df.columns[:-1]].to_markdown('output/ACCESS-OM2_runs_for_TMs.md')

df.to_csv('output/ACCESS-OM2_runs_for_TMs.csv')

# Print the runs that match what I need
print(catalog['1deg_jra55_iaf_omip2_cycle6'].metadata)
print(catalog['025deg_jra55_iaf_omip2_cycle6'].metadata)
print(catalog['01deg_jra55v140_iaf_cycle4'].metadata) # Note this is cycle 4 and not cycle 6
