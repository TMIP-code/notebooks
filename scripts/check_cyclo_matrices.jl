# This script just prints a tiny table that shows the "completion rate" of making  variables
using Format
using DataFrames
using DataFramesMeta
datadir = "/scratch/xv83/TMIP/data"
modeldir = joinpath(datadir, "ACCESS-ESM1-5")
decades = [1850, 1990, 2030, 2090]
# decades = [1990]
experiments = [t < 2010 ? "historical" : "ssp370" for t in decades]
files = ("cyclo_matrix_$i.jld2" for i in 1:12)
members = map(m -> "r$(m)i1p1f1", 1:40)
df = DataFrame(experiment = String[], decade = Int64[], member = String[], file = String[], isfile = Bool[])
for (decade, experiment) in zip(decades, experiments)
    experimentdir = joinpath(modeldir, experiment)
    for member in members
        dir = joinpath(experimentdir, member, "Jan$(decade)-Dec$(decade + 9)", "cyclomonth")
        for file in files
            filepath = joinpath(dir, file)
            row = (; experiment, decade, member, file, isfile = isfile(filepath))
            push!(df, row)
        end
    end
end
df
Nmembers = length(members)
Nfiles = length(files)
df2 = @chain df begin
    # @groupby(:decade)
    # @combine(:count => sum(:isfile))
    # @by(:decade, :done = 100 * sum(:isfile) / (Nmembers * Nvars))
    # @by([:decade], :count = "$(sum(:isfile)/Nfiles)/40", :done = (sum(:isfile) == Nmembers * Nfiles) ? "✓" : "")
    @by([:decade, :member], :done = (sum(:isfile) == Nfiles) ? "✓" : "")
end
show(df2, allrows = true)

using Test
@testset "Matrices" begin
    @testset "$experiment $decade" for (decade, experiment) in zip(decades, experiments)
        experimentdir = joinpath(modeldir, experiment)
        @testset "$member" for member in members
            dir = joinpath(experimentdir, member, "Jan$(decade)-Dec$(decade + 9)", "cyclomonth")
            @test all(isfile(joinpath(dir, file)) for file in files)
            # @testset "$member" for file in files
            #     filepath = joinpath(dir, file)
            #     @test isfile(filepath)
            # end
        end
    end
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
println("done")