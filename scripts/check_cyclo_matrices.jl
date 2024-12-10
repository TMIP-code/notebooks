# This script just prints a tiny table that shows the "completion rate" of making  variables
using Format
using Test
datadir = "/scratch/xv83/TMIP/data"
modeldir = joinpath(datadir, "ACCESS-ESM1-5")
decades = [1850, 1990, 2030, 2090]
experiments = [t < 2010 ? "historical" : "ssp370" for t in decades]
files = ("cyclo_matrix_$i.jld2" for i in 1:12)
members = map(m -> "r$(m)i1p1f1", 1:40)

@testset "Matrices" begin
    @testset "$experiment $decade" for (decade, experiment) in zip(decades, experiments)
        experimentdir = joinpath(modeldir, experiment)
        @testset "$member" for member in members
            dir = joinpath(experimentdir, member, "Jan$(decade)-Dec$(decade + 9)", "cyclomonth")
            @test all(isfile(joinpath(dir, file)) for file in files)
        end
    end
end

nothing