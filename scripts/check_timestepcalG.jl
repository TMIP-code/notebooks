# This script just prints a tiny table that shows the "completion rate" of making  variables
using Format
using Test
datadir = "/scratch/xv83/TMIP/data"
modeldir = joinpath(datadir, "ACCESS-ESM1-5")
decades = [2030, 2090]
experiments = [t < 2010 ? "historical" : "ssp370" for t in decades]
files = ("seqeff_centered_kVdeep3e-05_kH300_kVML1e+00_$(format(i, width=2, zeropadding=true)).nc" for i in 1:12)
members = map(m -> "r$(m)i1p1f1", 1:40)

@testset "time steppers" begin
    @testset "$experiment $decade" for (decade, experiment) in zip(decades, experiments)
        experimentdir = joinpath(modeldir, experiment)
        @testset "$member" for member in members
            @testset "final month $i" for i in 1:12
                dir = joinpath(experimentdir, member, "Jan$(decade)-Dec$(decade + 9)")
                file = "seqeff_centered_kVdeep3e-05_kH300_kVML1e+00_$(format(i, width=2, zeropadding=true)).nc"
                @test isfile(joinpath(dir, file))
            end
            dir = joinpath(experimentdir, member, "Jan$(decade)-Dec$(decade + 9)")
            isdone = [isfile(joinpath(dir, file)) for file in files]
            # @test all(isdone)
            if all(.!isdone)
                println("$experiment $decade $member: all files missing")
            elseif any(.!isdone)
                println("$experiment $decade $member: final months $(findall(.!isdone)) missing")
            end
        end
    end
end

nothing