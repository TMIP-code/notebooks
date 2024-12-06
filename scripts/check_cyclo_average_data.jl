# This script just prints a tiny table that shows the "completion rate" of making  variables
using Format
using DataFrames
using DataFramesMeta
datadir = "/scratch/xv83/TMIP/data"
modeldir = joinpath(datadir, "ACCESS-ESM1-5")
decades = [1850, 1990, 2020, 2030, 2090]
# decades = [1990]
experiments = [t < 2010 ? "historical" : "ssp370" for t in decades]
vars = ("umo", "vmo", "mlotst", "tx_trans_gm", "ty_trans_gm", "tx_trans_submeso", "ty_trans_submeso")
members = map(m -> "r$(m)i1p1f1", 1:40)
df = DataFrame(experiment = String[], decade = Int64[], member = String[], variable = String[], isfile = Bool[])
for (decade, experiment) in zip(decades, experiments)
    experimentdir = joinpath(modeldir, experiment)
    for member in members
        dir = joinpath(experimentdir, member, "Jan$(decade)-Dec$(decade + 9)", "cyclomonth")
        for variable in vars
            fname = joinpath(dir, "$variable.nc")
            row = (; experiment, decade, member, variable, isfile = isfile(fname))
            push!(df, row)
        end
    end
end
df
Nmembers = length(members)
Nvars = length(vars)
df2 = @chain df begin
    # @groupby(:decade)
    # @combine(:count => sum(:isfile))
    @by(:decade, :done = 100 * sum(:isfile) / (Nmembers * Nvars))
end
# df2 = @chain df begin
#     # @subset(:isfile)
#     # @rtransform(:filesize = round(:filesize, sigdigits=2))
#     # @select(:variable, :filesize)
#     @groupby(:decade)
#     @combine(:count => sum(:isfile))
#     # @transform(:maxsize = maximum(:filesize))
#     # @rsubset(!isapprox(:filesize, :maxsize, atol = 0.1 * :maxsize), :decade ≠ 2010)
#     # @rselect(:CSIRO_member, :decade, :variable, :potentially_missing_data = round(Int, pc, 1 - :filesize / :maxsize |> pc))
#     # @orderby(:CSIRO_member, :decade, :variable)
# end
show(df2, allrows = true)
exit()