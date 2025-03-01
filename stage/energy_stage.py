from data_tools.schema import FileLoader
from stage.stage import Stage
from stage.stage_registry import stage_registry
from data_tools.schema import Result, UnwrappedError, File, FileType, CanonicalPath
from data_tools.collections import TimeSeries
from prefect import task
from scipy.interpolate import CubicSpline
from typing import Callable


# lookup table mapping cell voltage to cell energy, derived from SANYO NCR18650GA datasheet 2A discharge curve.
# see https://github.com/UBC-Solar/data_analysis/blob/main/soc_analysis/datasheet_voltage_soc/charge_voltage_energy.ipynb
voltage_wh_lookup = [
    [2.701298701298701, 2.7190057294551995, 2.736712757611698, 2.7544197857681967, 2.7721268139246953, 2.789833842081194, 2.8061265387918692, 2.821000442443328, 2.8358743460947866, 2.8507482497462453, 2.8656221533977044, 2.880496057049163, 2.895369960700622, 2.9102438643520805, 2.9216955389300074, 2.9312300925527377, 2.9407646461754675, 2.9502991997981973, 2.9598337534209276, 2.9693683070436574, 2.978902860666387, 2.988437414289117, 2.9979719679118473, 3.007506521534577, 3.016913888893141, 3.026149319264962, 3.0353847496367825, 3.0446201800086037, 3.0538556103804244, 3.063091040752245, 3.072326471124066, 3.081561901495887, 3.0907973318677078, 3.1000327622395285, 3.1092681926113492, 3.1185036229831704, 3.127739053354991, 3.1367735470941884, 3.142971006948963, 3.1491684668037374, 3.1553659266585115, 3.161563386513286, 3.1677608463680604, 3.173958306222835, 3.1801557660776094, 3.186353225932384, 3.1925506857871584, 3.1987481456419324, 3.204945605496707, 3.2111430653514814, 3.217340525206256, 3.2235379850610304, 3.229735444915805, 3.234771066695785, 3.2376452509762603, 3.2405194352567355, 3.2433936195372106, 3.2462678038176858, 3.249141988098161, 3.252016172378636, 3.254890356659111, 3.2577645409395863, 3.2606387252200615, 3.2635129095005366, 3.2663870937810118, 3.269261278061487, 3.272135462341962, 3.275009646622437, 3.2778838309029124, 3.2807580151833875, 3.283632199463863, 3.2864073714905815, 3.288922282735997, 3.291437193981413, 3.2939521052268286, 3.2964670164722443, 3.29898192771766, 3.3014968389630757, 3.3040117502084914, 3.3065266614539075, 3.309041572699323, 3.311556483944739, 3.3140713951901546, 3.3165863064355703, 3.319101217680986, 3.3216161289264017, 3.3241310401718174, 3.326645951417233, 3.3291608626626488, 3.331513452341315, 3.3332230964391836, 3.3349327405370524, 3.3366423846349216, 3.3383520287327904, 3.340061672830659, 3.341771316928528, 3.3434809610263967, 3.3451906051242655, 3.3469002492221342, 3.348609893320003, 3.350319537417872, 3.3520291815157406, 3.3537388256136094, 3.3554484697114786, 3.3571581138093474, 3.358867757907216, 3.360577402005085, 3.3622870461029537, 3.3639966902008225, 3.3657063342986913, 3.36741597839656, 3.369125622494429, 3.3708604593230986, 3.372631162138749, 3.3744018649543985, 3.3761725677700483, 3.3779432705856984, 3.379713973401348, 3.381484676216998, 3.3832553790326476, 3.385026081848298, 3.3867967846639475, 3.3885674874795972, 3.3903381902952474, 3.392108893110897, 3.393879595926547, 3.395650298742197, 3.3974210015578468, 3.3991917043734965, 3.4009624071891467, 3.4027331100047964, 3.404503812820446, 3.4062745156360963, 3.408045218451746, 3.4094813002628634, 3.4104347556251366, 3.4113882109874094, 3.412341666349682, 3.4132951217119554, 3.414248577074228, 3.4152020324365013, 3.416155487798774, 3.4171089431610473, 3.41806239852332, 3.4190158538855933, 3.419969309247866, 3.420922764610139, 3.421876219972412, 3.4228296753346847, 3.423783130696958, 3.4247365860592307, 3.425690041421504, 3.4266434967837767, 3.42759695214605, 3.4285504075083226, 3.430510891913697, 3.432494079067225, 3.434477266220753, 3.4364604533742806, 3.4384436405278085, 3.4404268276813363, 3.442410014834864, 3.4443932019883916, 3.4463763891419195, 3.4483595762954478, 3.4503427634489756, 3.452325950602503, 3.454309137756031, 3.4562923249095587, 3.4582755120630866, 3.4602586992166144, 3.4622418863701423, 3.46422507352367, 3.466208260677198, 3.4679717877313063, 3.469293912500325, 3.4706160372693438, 3.471938162038362, 3.4732602868073807, 3.4745824115763995, 3.4759045363454177, 3.4772266611144365, 3.478548785883455, 3.4798709106524734, 3.481193035421492, 3.482515160190511, 3.483837284959529, 3.485159409728548, 3.4864815344975666, 3.487803659266585, 3.4891257840356036, 3.4904479088046223, 3.4917700335736406, 3.4930921583426593, 3.494641230512974, 3.496293886474247, 3.4979465424355203, 3.4995991983967936, 3.501251854358067, 3.50290451031934, 3.504557166280613, 3.5062098222418863, 3.5078624782031596, 3.509515134164433, 3.5111677901257057, 3.512820446086979, 3.5144731020482523, 3.5161257580095255, 3.5177784139707984, 3.5194310699320717, 3.521083725893345, 3.5227363818546182, 3.524389037815891, 3.5260556940122965, 3.5280502787931436, 3.5300448635739903, 3.5320394483548374, 3.5340340331356845, 3.5360286179165312, 3.5380232026973784, 3.5400177874782255, 3.542012372259072, 3.5440069570399193, 3.546001541820766, 3.547996126601613, 3.5499907113824603, 3.551985296163307, 3.553979880944154, 3.555974465725001, 3.557969050505848, 3.559963635286695, 3.5619582200675417, 3.563952804848389, 3.565947389629236, 3.5679419744100827, 3.56993655919093, 3.571859347894176, 3.573568991992045, 3.5752786360899136, 3.5769882801877824, 3.578697924285651, 3.58040756838352, 3.5821172124813887, 3.583826856579258, 3.5855365006771267, 3.5872461447749955, 3.5889557888728643, 3.590665432970733, 3.592375077068602, 3.5940847211664706, 3.5957943652643394, 3.5975040093622086, 3.5992136534600774, 3.600923297557946, 3.602632941655815, 3.6043425857536837, 3.6060522298515525, 3.6077618739494213, 3.60947151804729, 3.611445012730342, 3.613724538194167, 3.616004063657992, 3.6182835891218175, 3.6205631145856425, 3.6228426400494675, 3.6251221655132926, 3.6274016909771176, 3.6296812164409427, 3.6319607419047677, 3.6342402673685927, 3.6365197928324178, 3.638799318296243, 3.641078843760068, 3.643358369223893, 3.6456378946877184, 3.6479174201515434, 3.6501969456153684, 3.6524764710791935, 3.6547559965430185, 3.6570355220068436, 3.6593150474706686, 3.6615945729344936, 3.66335031337764, 3.664852727887889, 3.666355142398137, 3.6678575569083853, 3.669359971418634, 3.6708623859288823, 3.6723648004391305, 3.673867214949379, 3.6753696294596274, 3.6768720439698757, 3.678374458480124, 3.6798768729903726, 3.681379287500621, 3.682881702010869, 3.6843841165211177, 3.685886531031366, 3.6873889455416142, 3.6888913600518625, 3.690393774562111, 3.6918961890723594, 3.6933986035826076, 3.6949010180928563, 3.6964034326031046, 3.697905847113353, 3.6994082616236015, 3.7009106761338497, 3.7027845537592303, 3.704787773106228, 3.706790992453226, 3.7087942118002237, 3.7107974311472214, 3.7128006504942195, 3.714803869841217, 3.716807089188215, 3.718810308535213, 3.7208135278822105, 3.7228167472292086, 3.7248199665762063, 3.726823185923204, 3.728826405270202, 3.7308296246171997, 3.732832843964198, 3.7348360633111954, 3.736839282658193, 3.7388425020051907, 3.740845721352189, 3.7428489406991865, 3.7448521600461846, 3.7468553793931823, 3.74885859874018, 3.750861818087178, 3.7528650374341757, 3.7542757843359045, 3.7555470581522687, 3.7568183319686326, 3.7580896057849964, 3.7593608796013607, 3.7606321534177245, 3.7619034272340883, 3.7631747010504526, 3.7644459748668164, 3.7657172486831807, 3.7669885224995445, 3.7682597963159083, 3.7695310701322726, 3.7708023439486364, 3.7720736177650003, 3.7733448915813645, 3.7746161653977284, 3.775887439214092, 3.7771587130304565, 3.7784299868468203, 3.7801131018995315, 3.782474038987065, 3.784834976074598, 3.7871959131621313, 3.789556850249664, 3.7919177873371974, 3.7942787244247302, 3.7966396615122635, 3.7990005985997968, 3.80136153568733, 3.8037224727748633, 3.806083409862396, 3.8084443469499294, 3.8108052840374627, 3.8131662211249955, 3.8155271582125287, 3.817888095300062, 3.8202490323875953, 3.822609969475128, 3.8249709065626614, 3.8273318436501946, 3.829692780737728, 3.8317644279568124, 3.8333535202272677, 3.8349426124977226, 3.8365317047681775, 3.8381207970386324, 3.8397098893090873, 3.8412989815795426, 3.8428880738499975, 3.8444771661204524, 3.8460662583909073, 3.8476553506613627, 3.8492444429318176, 3.8508335352022725, 3.8524226274727273, 3.8540117197431822, 3.8556008120136376, 3.8571899042840925, 3.8587789965545474, 3.8603680888250023, 3.861957181095457, 3.863546273365912, 3.8655850662363687, 3.86765088618796, 3.8697167061395517, 3.871782526091143, 3.8738483460427346, 3.875914165994326, 3.8779799859459176, 3.880045805897509, 3.8821116258491006, 3.884177445800692, 3.8862432657522836, 3.888309085703875, 3.8903749056554666, 3.892440725607058, 3.8945065455586496, 3.896572365510241, 3.8986381854618326, 3.900704005413424, 3.9027698253650156, 3.9048356453166075, 3.906901465268199, 3.9089672852197905, 3.910584906075888, 3.912173998346343, 3.913763090616798, 3.9153521828872533, 3.916941275157708, 3.918530367428163, 3.920119459698618, 3.921708551969073, 3.923297644239528, 3.924886736509983, 3.926475828780438, 3.928064921050893, 3.929654013321348, 3.931243105591803, 3.932832197862258, 3.934421290132713, 3.936010382403168, 3.937599474673623, 3.939188566944078, 3.940777659214533, 3.9423990838820497, 3.944051739843323, 3.945704395804596, 3.947357051765869, 3.9490097077271424, 3.9506623636884157, 3.952315019649689, 3.953967675610962, 3.955620331572235, 3.9572729875335084, 3.9589256434947817, 3.9605782994560546, 3.962230955417328, 3.963883611378601, 3.9655362673398744, 3.9671889233011477, 3.9688415792624205, 3.970494235223694, 3.972146891184967, 3.9737995471462404, 3.9754522031075137, 3.9771048590687865, 3.97875751503006, 3.980410170991333, 3.9813317681158624, 3.9822015870428484, 3.9830714059698344, 3.98394122489682, 3.984811043823806, 3.985680862750792, 3.9865506816777776, 3.9874205006047636, 3.9882903195317496, 3.9891601384587356, 3.990029957385721, 3.990899776312707, 3.991769595239693, 3.9926394141666792, 3.9935084955125033, 3.9941441324206854, 3.9947797693288676, 3.9954154062370493, 3.9960510431452314, 3.9966866800534135, 3.9973223169615957, 3.9979579538697774, 3.9985935907779595, 3.9992292276861416, 3.9998648645943233, 4.004337679688381, 4.009846532892625, 4.015355386096869, 4.020864239301113, 4.026373092505357, 4.031881945709601, 4.037390798913845, 4.0428996521180895, 4.048408505322333, 4.078533690758138, 4.152903209015433, 4.1688311688311686],
    [0.017736001074258087, 0.035588261735156695, 0.05355678198269584, 0.0716415618168755, 0.08984260123769569, 0.10815990024515643, 0.12658417271920347, 0.14510610324602857, 0.1637256918256317, 0.1824429384580129, 0.20125784314317213, 0.2201704058811094, 0.2391806266718247, 0.258288505515318, 0.277471572975501, 0.2967172417515674, 0.3160255118435171, 0.3353963832513501, 0.3548298559750666, 0.37432593001466635, 0.3938846053701495, 0.413505882041516, 0.43318976002876586, 0.45293623933189914, 0.4727444848800971, 0.49261336778132714, 0.5125428880355892, 0.5325330456428836, 0.55258384060321, 0.5726952729165685, 0.5928673425829593, 0.613100049602382, 0.6333933939748368, 0.6537473757003238, 0.674161994778843, 0.6946372512103942, 0.7151731449949775, 0.7357683568366816, 0.7564042595337099, 0.7770808530860625, 0.7977981374937391, 0.8185561127567398, 0.8393547788750648, 0.860194135848714, 0.8810741836776873, 0.9019949223619848, 0.9229563519016065, 0.9439584722965524, 0.9650012835468225, 0.9860847856524169, 1.0072089786133354, 1.028373862429578, 1.0495794371011449, 1.070818074311577, 1.0920755826433195, 1.1133519620963714, 1.1346472126707332, 1.1559613343664046, 1.177294327183386, 1.198646191121677, 1.2200169261812777, 1.2414065323621883, 1.2628150096644086, 1.2842423580879385, 1.3056885776327785, 1.3271536682989282, 1.3486376300863876, 1.3701404629951568, 1.3916621670252356, 1.4132027421766244, 1.434762188449323, 1.4563398557554905, 1.4779340352928045, 1.4995447270612645, 1.5211719310608705, 1.5428156472916223, 1.5644758757535202, 1.5861526164465647, 1.607845869370755, 1.629555634526091, 1.6512819119125735, 1.6730247015302018, 1.6947840033789763, 1.7165598174588967, 1.7383521437699634, 1.7601609823121758, 1.7819863330855343, 1.8038281960900389, 1.8256865713256896, 1.8475603930327162, 1.8694454398032805, 1.8913417116373827, 1.9132492085350228, 1.9351679304962004, 1.9570978775209156, 1.9790390496091685, 2.0009914467609593, 2.022955068976288, 2.044929916255154, 2.0669159885975583, 2.0889132860035, 2.1109218084729786, 2.132941556005996, 2.1549725286025505, 2.1770147262626427, 2.199068148986273, 2.221132796773441, 2.2432086696241464, 2.2652957675383893, 2.287394090516171, 2.309503638557489, 2.3316244116623457, 2.3537565752394376, 2.3759003647751933, 2.3980557802696136, 2.420222821722697, 2.4424014891344457, 2.4645917825048578, 2.4867937018339337, 2.5090072471216742, 2.5312324183680786, 2.5534692155731467, 2.575717638736879, 2.597977687859275, 2.620249362940336, 2.6425326639800604, 2.6648275909784487, 2.6871341439355017, 2.709452322851218, 2.7317821277255985, 2.7541235585586437, 2.7764766153503526, 2.7988412981007253, 2.821217606809762, 2.8436033444459046, 2.8659953422136355, 2.8883936001129547, 2.9107981181438616, 2.9332088963063576, 2.955625934600442, 2.978049233026114, 3.000478791583375, 3.0229146102722244, 3.045356689092662, 3.0678050280446874, 3.0902596271283014, 3.1127204863435036, 3.1351876056902945, 3.157660985168674, 3.1801406247786415, 3.202626524520197, 3.225118684393341, 3.2476171043980733, 3.2701217845343944, 3.2926327248023037, 3.3151565370837752, 3.337693370438951, 3.3602432248678302, 3.382806100370413, 3.4053819969466996, 3.42797091459669, 3.4505728533203843, 3.4731878131177822, 3.4958157939888843, 3.51845679593369, 3.541110818952199, 3.563777863044412, 3.5864579282103284, 3.6091510144499486, 3.6318571217632725, 3.654576250150301, 3.6773083996110327, 3.700053570145468, 3.722811761753607, 3.745581532206289, 3.768359983374774, 3.7911471152590606, 3.8139429278591495, 3.836747421175042, 3.8595605952067364, 3.882382449954233, 3.905212985417532, 3.9280522015966346, 3.9509000984915392, 3.973756676102246, 3.996621934428755, 4.019495873471067, 4.042378493229181, 4.065269793703099, 4.088169774892817, 4.11107843679834, 4.133995779419664, 4.1569218027567905, 4.1798565068097195, 4.202801381654078, 4.225757107393188, 4.248723684027053, 4.27170111155567, 4.29468938997904, 4.317688519297164, 4.34069849951004, 4.36371933061767, 4.386751012620052, 4.409793545517188, 4.432846929309076, 4.455911163995718, 4.478986249577113, 4.502072186053261, 4.5251689734241625, 4.5482766116898175, 4.571395100850224, 4.594524440905386, 4.617664631855299, 4.640815765621746, 4.663979995295653, 4.687157320877023, 4.71034774236585, 4.73355125976214, 4.75676787306589, 4.779997582277101, 4.803240387395773, 4.826496288421904, 4.849765285355497, 4.87304737819655, 4.896342566945064, 4.919650851601039, 4.942972232164474, 4.96630670863537, 4.989654281013727, 5.013014949299545, 5.036388713492824, 5.059775573593562, 5.083175529601763, 5.106588581517423, 5.1300147293405445, 5.153453973071127, 5.176905841315423, 5.20036893462326, 5.223843252994633, 5.247328796429544, 5.270825564927993, 5.294333558489979, 5.317852777115503, 5.341383220804564, 5.364924889557163, 5.3884777833733, 5.412041902252976, 5.4356172461961885, 5.459203815202939, 5.482801609273227, 5.506410628407053, 5.530030872604416, 5.5536623418653175, 5.577305036189756, 5.600958955577732, 5.624624100029247, 5.648300469544298, 5.671988064122888, 5.695686883765015, 5.719398660842692, 5.74312540467175, 5.766867115252194, 5.79062379258402, 5.81439543666723, 5.838182047501823, 5.861983625087801, 5.885800169425161, 5.909631680513906, 5.933478158354034, 5.957339602945545, 5.981216014288441, 6.00510739238272, 6.029013737228382, 6.052935048825429, 6.076871327173858, 6.100822572273672, 6.124788784124869, 6.148769962727449, 6.172766108081413, 6.196777220186761, 6.220803299043492, 6.244844344651606, 6.268896917979471, 6.29295935575711, 6.317031657984525, 6.341113824661716, 6.365205855788681, 6.389307751365423, 6.41341951139194, 6.437541135868233, 6.461672624794301, 6.485813978170144, 6.509965195995763, 6.534126278271158, 6.558297224996328, 6.582478036171273, 6.606668711795994, 6.63086925187049, 6.655079656394763, 6.679299925368812, 6.703530058792635, 6.727770056666235, 6.75201991898961, 6.77627964576276, 6.800549236985686, 6.824828692658388, 6.849118012780865, 6.873417197353118, 6.8977286853020985, 6.92205332585078, 6.946391118999163, 6.970742064747246, 6.99510616309503, 7.019483414042514, 7.0438738175896995, 7.068277373736585, 7.092694082483172, 7.117123943829459, 7.141566957775447, 7.1660231243211365, 7.190492443466526, 7.214974915211616, 7.239470539556406, 7.2639793165008975, 7.28850124604509, 7.313036328188982, 7.337584562932576, 7.3621459502758695, 7.386720490218865, 7.411308182761561, 7.435909027903956, 7.4605230256460535, 7.485150175987853, 7.509790478929353, 7.534440044455753, 7.559097956824271, 7.583764216034907, 7.608438822087661, 7.633121774982532, 7.657813074719522, 7.682512721298629, 7.707220714719854, 7.731937054983195, 7.756661742088656, 7.781394776036234, 7.80613615682593, 7.830885884457745, 7.855643958931675, 7.880410380247725, 7.905185148405893, 7.929968263406177, 7.95475972524858, 7.979559533933101, 8.00436768945974, 8.029186895867337, 8.054021603553155, 8.078871812517189, 8.103737522759442, 8.128618734279915, 8.153515447078606, 8.178427661155515, 8.203355376510645, 8.228298593143991, 8.253257311055556, 8.278231530245343, 8.303221250713344, 8.328226472459567, 8.353247195484009, 8.37828341978667, 8.403335145367546, 8.428402372226643, 8.45348510036396, 8.478583329779495, 8.503697060473247, 8.528826292445219, 8.55397102569541, 8.579129360824314, 8.604298129505866, 8.629477331740064, 8.65466696752691, 8.679867036866405, 8.705077539758545, 8.73029847620333, 8.755529846200766, 8.780771649750847, 8.806023886853577, 8.831286557508953, 8.856559661716975, 8.881843199477647, 8.907137170790966, 8.93244157565693, 8.957756414075542, 8.983081686046802, 9.008417391570708, 9.033763530647262, 9.059120103276463, 9.084487109458312, 9.109867501806054, 9.135261457772236, 9.16066897735686, 9.186090060559925, 9.211524707381432, 9.23697291782138, 9.26243469187977, 9.2879100295566, 9.313398930851873, 9.338901395765587, 9.364417424297743, 9.389947016448339, 9.415490172217376, 9.441046891604856, 9.466617174610775, 9.49220102123514, 9.517798431477942, 9.543409405339187, 9.569033942818873, 9.594672043917, 9.62032370863357, 9.64598893696858, 9.671664786167263, 9.697351068918591, 9.723047785222567, 9.74875493507919, 9.77447251848846, 9.80020053545038, 9.825938985964944, 9.851687870032157, 9.877447187652015, 9.90321693882452, 9.928997123549674, 9.954787741827476, 9.980588793657924, 10.006400279041019, 10.032222197976761, 10.058054550465153, 10.083897336506189, 10.109750556099872, 10.135614209246203, 10.161488295945183, 10.187373028482634, 10.213268611914842, 10.239175046241801, 10.265092331463515, 10.29102046757998, 10.3169594545912, 10.34290929249717, 10.368869981297895, 10.394841520993372, 10.420823911583604, 10.446817153068586, 10.472821245448324, 10.498836188722814, 10.524861982892059, 10.550898627956055, 10.576946123914803, 10.603004470768308, 10.629073668516565, 10.655153717159573, 10.681244616697336, 10.707346367129853, 10.73345896845712, 10.759582420679141, 10.785716723795916, 10.811857077871654, 10.83800314294463, 10.864154919014847, 10.890312406082302, 10.916475604146994, 10.942644513208926, 10.968819133268095, 10.9949994643245, 11.021185506378147, 11.047377259429034, 11.073574723477156, 11.099777898522518, 11.125986784565118, 11.152201381604959, 11.178421684799275, 11.204646161414653, 11.230874811451088, 11.257107634908582, 11.283344631787134, 11.309585802086747, 11.335831145807418, 11.362080662949147, 11.388334353511937, 11.414592217495784, 11.44085425490069, 11.467145659607066, 11.493473233962614, 11.519836977967342, 11.546236891621247, 11.572672974924329, 11.599145227876589, 11.625653650478023, 11.652198242728637, 11.678779004628426, 11.705557560397141, 11.732824406429748, 11.76019583116454]
]

# battery configuration: 32s13p
cells_in_module = 13  # parallel
modules_in_pack = 32  # series


class EnergyStage(Stage):
    @classmethod
    def get_stage_name(cls):
        return "Energy"

    @staticmethod
    def dependencies():
        return ["ingress"]

    @staticmethod
    @task(name="Energy")
    def run(self, voltage_of_least_loader: FileLoader) -> tuple[FileLoader, ...]:
        """
        Run the Energy stage, producing battery energy estimates using various techniques.

        1. EnergyVOLExtrapolated
            Battery energy estimated using VoltageofLeast & SANYO NCR18650GA datasheet 2A discharge curve.
            See https://github.com/UBC-Solar/data_analysis/blob/main/soc_analysis/datasheet_voltage_soc/charge_voltage_energy.ipynb for details.

        :param EnergyStage self: an instance of EnergyStage to be run
        :param FileLoader voltage_of_least_loader: loader to VoltageofLeast from Ingest
        :returns: EnergyVOLExtrapolated (FileLoader pointing to TimeSeries)
        """
        return super().run(self,voltage_of_least_loader)

    @property
    def event_name(self):
        return self._event_name

    def __init__(self, event_name: str):
        """
        :param str event_name: which event is currently being processed
        """
        super().__init__()

        self._event_name = event_name

    def extract(self, voltage_of_least_loader: FileLoader) -> tuple[Result]:
        voltage_of_least_result: Result = voltage_of_least_loader()

        return (voltage_of_least_result,)

    def transform(self, voltage_of_least_result) -> tuple[Result]:
        try:
            voltage_of_least: TimeSeries = voltage_of_least_result.unwrap().data

            cell_wh_from_voltage: Callable = CubicSpline(voltage_wh_lookup[0], voltage_wh_lookup[1])
            cell_wh= cell_wh_from_voltage(voltage_of_least)
            extrapolated_pack_wh: TimeSeries = voltage_of_least.promote(cell_wh * cells_in_module * modules_in_pack)
            extrapolated_pack_wh.name = "EnergyVOLExtrapolated"
            extrapolated_pack_wh.units = "Wh"

            energy_vol_extrapolated = Result.Ok(extrapolated_pack_wh)

        except UnwrappedError as e:
            self.logger.error(f"Failed to unwrap result! \n {e}")
            energy_vol_extrapolated = Result.Err(RuntimeError("Failed to process EnergyVOLExtrapolated!"))

        return (energy_vol_extrapolated,)

    def load(self, energy_vol_extrapolated) -> tuple[FileLoader]:
        energy_vol_extrapolated_file = File(
            canonical_path=CanonicalPath(
                origin=self.context.title,
                event=self.event_name,
                source=EnergyStage.get_stage_name(),
                name="EnergyVOLExtrapolated",
            ),
            file_type=FileType.TimeSeries,
            data=energy_vol_extrapolated.unwrap() if energy_vol_extrapolated else None,
            description = "Battery energy estimated using VoltageofLeast & SANYO NCR18650GA datasheet 2A discharge curve."
                          "\nSee https://github.com/UBC-Solar/data_analysis/blob/main/soc_analysis/datasheet_voltage_soc/charge_voltage_energy.ipynb for details."
        )

        energy_vol_extrapolated_loader = self.context.data_source.store(energy_vol_extrapolated_file)
        self.logger.info(f"Successfully loaded EnergyVOLExtrapolated!")


        return (energy_vol_extrapolated_loader,)


stage_registry.register_stage(EnergyStage.get_stage_name(), EnergyStage)
