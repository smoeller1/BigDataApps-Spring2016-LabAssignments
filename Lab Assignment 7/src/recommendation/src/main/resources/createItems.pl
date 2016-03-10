use strict;

my $c = 0;

my @brands = ('Gap','Old Navy','Hot Topic','Aero','Brooks Brothers');
my @clothing = ('shorts','pants','t-shirt','polo','golf shirt','black socks','wind breaker');

foreach my $brand (@brands) {
  foreach my $item (@clothing) {
    printf("%d::%s::%s\n", ++$c, $item, $brand);
  }
}
