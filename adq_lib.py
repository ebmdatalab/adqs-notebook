import functools
import re
import pandas as pd



## Data generation
def normalised_dmd(csv):
    # Normalise numerators and denominators from DMD in the same way
    dmd = pd.read_csv(csv)
    normalise_numerator = functools.partial(
        normalise, number_name='numerator',
        unit_name='numerator_unit_of_measure')
    dmd = dmd.apply(normalise_numerator, axis=1)
    normalise_denominator = functools.partial(
        normalise, number_name='denominator',
        unit_name='denominator_unit_of_measure')
    dmd = dmd.apply(normalise_denominator, axis=1)
    return dmd


def normalised_adqs(csv):
    # Import ADQs (which we got from an FOI)
    adqs = pd.read_csv(csv)
    adqs.columns = ['bnf_code', 'bnf_name', 'adq_value', 'adq_denominator']
    adqs = adqs[~pd.isnull(adqs['adq_value'])]

    # Extract Dose info from the name. For example, `Seretide
    # 500_Accuhaler 500mcg/50mcg(60D)`, is expressed in quantities of
    # 1 pack. We have to multiply that by 60 to work out ADQs
    adqs['dose_multiplier'] = adqs['bnf_name'].apply(dose_from_name)

    # Normalise units to grammes, ml, units, or unit doses
    normalise_adq = functools.partial(
        normalise, number_name='adq_value', unit_name='adq_denominator')
    return adqs.apply(normalise_adq, axis=1)



## Unit calculations

# work out quantity_units
def compute_quantity_units(row):
    # Definition of solid-continuous products from
    # https://github.com/ebmdatalab/openprescribing/issues/937

    # Very few of these are consistently measured in ml or g.
    # but this heuristic represents the best guess based on comparing with the SQU data we've found.
    # Pastilles, Ointments, Inhalation Vapour are particuarly contested as to g or ml.
    solid_continuous = [
        'Wash',
        'Granules',
        'Paste',  # Can be both
        'Rectal ointment',
        'Stick',
        'Cream',
        'Oromucosal gel',
        'Oral gel',
        'Powder',
        'Nasal ointment',
        'Poultice',
        'Eye gel',
        'Vaginal gel',
        'Eye ointment',
        'Powder for solution for iontophoresis',
        'Effervescent powder',
        'Impregnated dressing',
        'Effervescent granules',
        'Gastro-resistant granules',
        'Gel',
        'Ointment',
        'Foam'
    ]
    if row.squ:
        unit = row.squ
    else:
        unit = None
        if row.form_indicator == 'Not applicable' or row.form == 'Not applicable':
            # Not clear what "Not applicable" means as there are clearly
            # some sachets etc; that said, most appear to be wierd things
            # like poultices or whatnot.  In all these cases,
            # unit_of_measure is also null.
            unit = None
        elif row.form in solid_continuous:
            unit = 'g'
        elif row.form_units == 'litre':
            unit = 'ml'
        elif row.form_indicator == 'Discrete' and row.unit_of_measure != 'spoonful':
            # Could we use form_units here, if set?
            unit = row.unit_of_measure
        else:
            unit = 'ml'
    return unit


def normalise(row, number_name='', unit_name=''):
    """Notes from NHSBSA in FOI response:

    Please note if N/A in ADQ usage column this indicates that there
    is no ADQ value available and therefore no ADQ usage can be
    calculated.

    If a strength is u/ml and the amount of units is equal to or more
    than 10,000u/ml the strength field on MDR cannot hold it. At this
    point the TE is used which is the equivalent of 1,000u. For
    example 10,000u/ml would be represented as 10.000TE/ml and
    20,000u/ml would be 20.000TE/ml, the TE (Therapeutic Equivalent)
    does not affect the ADQ value.

    """
    # XXX sometimes returning the string `nan`
    unit = str(row[unit_name]).lower().strip()
    # for numerator, can be ['mg', 'microgram', 'microlitre', 'ml', 'gram', 'mmol', nan, 'unit']
    if isinstance(row[number_name], str):
        number = float(row[number_name].replace(',', ''))
    else:
        number = row[number_name]
    if unit == 'mega u' or unit == 'u':
        unit = 'unit dose'
        number = number / 1000.0  # poss 1000000?
    elif unit == 'mcg' or unit == 'microgram':
        unit = 'g'
        number = number / 1000.0 / 1000
    elif unit == 'mg':
        unit = 'g'
        number = number / 1000.0
    elif unit == 'gramme' or unit == 'gram':
        unit = 'g'
    elif unit == 'te':
        unit = 'unit dose'
        number = number * 1000
    elif unit == 'puffs':
        unit = 'dose'  # to match dm+d terminology
    elif unit == 'microlitre':
        number = number / 1000.0
        unit = 'ml'
    elif unit == 'litre':
        number = number * 1000
        unit = 'ml'
    row[number_name] = number
    row[unit_name] = unit
    return row


def adq_per_quantity(row):
    numerator = row.numerator
    # ADQs are sometimes measured in dose, but this means the same as unit
    adq_denominator = row.adq_denominator.replace("dose", "unit")
    quantity_denominator = row.quantity_denominator
    quantity_in_adq_units = None
    if pd.isnull(row.form_size):
        form_size = 1
    else:
        form_size = row.form_size
    if row.denominator_unit_of_measure == 'hour':
        # These are patches, normally denominated in patches per day, except:
        if row.adq_denominator == 'g':
            # XXX can't work this branch out - we never match NHS
            # Digital's supplied values.  See "Some patches just don't
            # make sense" in notes.md.  For now, short circuit
            return
    if adq_denominator == '':
        # When the ADQ units haven't been specified, 96% of the time
        # it's a "unit" (e.g. sachets, etc)
        adq_denominator = quantity_denominator
    if adq_denominator == quantity_denominator:
        # nearly always things specified in units (per above); the
        # remainder include some data with errors (see notes.md) and a
        # handful of correct exceptions
        if row.unit_of_measure == 'unit dose':
            quantity_in_adq_units = row.form_size
        else:
            quantity_in_adq_units = 1
    else:
        if quantity_denominator == 'unit':
            if row.ingredient_count == 1.0:
                # the ADQ is always in g, ml, dose or unit dose
                if adq_denominator == row.numerator_unit_of_measure:
                    # (here ADQ will be "ml" or "g")
                    quantity_in_adq_units = form_size * numerator
                else:
                    # (Here ADQ will be "dose" or "unit dose")
                    quantity_in_adq_units = numerator
            else:
                # The quantity of the ADQ is only available in the
                # name. There's only 6 such drugs at the moment, e.g.
                # `Maxepa_Cap 1g`
                match = re.match(
                    r".*\b(\d+){}\b".format(row.adq_denominator), row.bnf_name)
                if match:
                    quantity_in_adq_units = float(match.groups()[0])
        else:
            quantity_in_adq_units = numerator
    if quantity_in_adq_units:
        return (quantity_in_adq_units * float(row.dose_multiplier)) / row.adq_value


def dose_from_name(name):
    # Formats include:
    #   Mesalazine_Foam Aero Enem 1g/D 14g
    #   Seretide 500_Accuhaler 500mcg/50mcg(60D)
    match = re.match(r'.*(?:(?:\b|mg|mcg|ml)(\d+) ?D\b|\d.{1,5}/D (\d+))', name)
    if match:
        for group in match.groups():
            if group:
                return float(group)
    else:
        return 1.0


def generate_adqs_per_quantity():
    squs = pd.read_csv(SQUS_CSV)
    df = normalised_dmd().merge(squs, on='bnf_code')
    df = df.merge(normalised_adqs(), on='bnf_code')
    # Compute the quantity_denominator. This prefers SQUs from data
    # but where this is missing, uses a heuristic to guess.
    df['quantity_denominator'] = df.apply(compute_quantity_units, axis=1)
    # This is where the main logic happens
    df['adq_per_quantity'] = df.apply(adq_per_quantity, axis=1)
    df.to_csv("computed_adqs.csv")


if __name__ == '__main__':
    generate_adqs_per_quantity()
    print("Created file at `computed_adqs.csv`")
