
class MicrocontrollerDataset(utils.Dataset):
    def load_dataset(self, dataset_dir):
        self.add_class('dataset', 1, 'balloon')
        self.add_class('dataset', 2, 'motorcycle')
        self.add_class('dataset', 3, 'horse')
        self.add_class('dataset', 4, 'hat')
        self.add_class('dataset', 5, 'train')
        self.add_class('dataset', 6, 'truck')
        self.add_class('dataset', 7, 'rose')
        self.add_class('dataset', 8, 'bus')
        self.add_class('dataset', 9, 'desk')
        self.add_class('dataset', 10, 'cattle')
        self.add_class('dataset', 11, 'bee')
        self.add_class('dataset', 12, 'tie')
        self.add_class('dataset', 13, 'butterfly')
        self.add_class('dataset', 14, 'swimwear')
        self.add_class('dataset', 15, 'billboard')
        self.add_class('dataset', 16, 'goggles')
        self.add_class('dataset', 17, 'beer')
        self.add_class('dataset', 18, 'laptop')
        self.add_class('dataset', 19, 'cabinetry')
        self.add_class('dataset', 20, 'insect')

        self.add_class('dataset', 21, 'stairs')
        self.add_class('dataset', 22, 'candle')
        self.add_class('dataset', 23, 'pastry')
        self.add_class('dataset', 24, 'cake')
        self.add_class('dataset', 25, 'lantern')
        self.add_class('dataset', 26, 'plate')
        self.add_class('dataset', 27, 'box')
        self.add_class('dataset', 28, 'bookcase')
        self.add_class('dataset', 29, 'watercraft')
        self.add_class('dataset', 30, 'football')
        self.add_class('dataset', 31, 'maple')
        self.add_class('dataset', 32, 'curtain')
        self.add_class('dataset', 33, 'muffin')
        self.add_class('dataset', 34, 'canoe')
        self.add_class('dataset', 35, 'swan')
        self.add_class('dataset', 36, 'bowl')
        self.add_class('dataset', 37, 'mushroom')
        self.add_class('dataset', 38, 'cocktail')
        self.add_class('dataset', 39, 'drawer')
        self.add_class('dataset', 40, 'castle')

        self.add_class('dataset', 41, 'couch')
        self.add_class('dataset', 42, 'taxi')
        self.add_class('dataset', 43, 'penguin')
        self.add_class('dataset', 44, 'cookie')
        self.add_class('dataset', 45, 'apple')
        self.add_class('dataset', 46, 'van')
        self.add_class('dataset', 47, 'shirt')
        self.add_class('dataset', 48, 'bench')
        self.add_class('dataset', 49, 'umbrella')
        self.add_class('dataset', 50, 'paddle')
        self.add_class('dataset', 51, 'deer')
        self.add_class('dataset', 52, 'porch')
        self.add_class('dataset', 53, 'bread')
        self.add_class('dataset', 54, 'television')
        self.add_class('dataset', 55, 'fountain')
        self.add_class('dataset', 56, 'doll')
        self.add_class('dataset', 57, 'camera')
        self.add_class('dataset', 58, 'tomato')
        self.add_class('dataset', 59, 'orange')
        self.add_class('dataset', 60, 'Ppumpkin')

        self.add_class('dataset', 61, 'clothing')
        self.add_class('dataset', 62, 'man')
        self.add_class('dataset', 63, 'tree')
        self.add_class('dataset', 64, 'person')
        self.add_class('dataset', 65, 'woman')
        self.add_class('dataset', 66, 'footwear')
        self.add_class('dataset', 67, 'window')
        self.add_class('dataset', 68, 'flower')
        self.add_class('dataset', 69, 'wheel')
        self.add_class('dataset', 70, 'plant')
        self.add_class('dataset', 71, 'car')
        self.add_class('dataset', 72, 'salad')
        self.add_class('dataset', 73, 'building')
        self.add_class('dataset', 74, 'mammal')
        self.add_class('dataset', 75, 'house')
        self.add_class('dataset', 76, 'chair')
        self.add_class('dataset', 77, 'tire')
        self.add_class('dataset', 78, 'suit')
        self.add_class('dataset', 79, 'food')
        self.add_class('dataset', 80, 'boy')

        # find all images
        for i, filename in enumerate(os.listdir(dataset_dir)):
            if '.jpg' in filename:
                self.add_image('dataset',
                               image_id=i,
                               path=os.path.join(dataset_dir, filename),
                               annotation=os.path.join(dataset_dir, filename.replace('.jpg', '.xml')))

    # extract bounding boxes from an annotation file
    def extract_boxes(self, filename):
        # load and parse the file
        tree = ET.parse(filename)
        # get the root of the document
        root = tree.getroot()
        # extract each bounding box
        boxes = []
        classes = []
        for member in root.findall('object'):
            xmin = int(member[4][0].text)
            ymin = int(member[4][1].text)
            xmax = int(member[4][2].text)
            ymax = int(member[4][3].text)
            boxes.append([xmin, ymin, xmax, ymax])
            classes.append(self.class_names.index(member[0].text))
        # extract image dimensions
        width = int(root.find('size')[0].text)
        height = int(root.find('size')[1].text)
        return boxes, classes, width, height

    # load the masks for an image
    def load_mask(self, image_id):
        # get details of image
        info = self.image_info[image_id]
        # define box file location
        path = info['annotation']
        # load XML
        boxes, classes, w, h = self.extract_boxes(path)
        # create one array for all masks, each on a different channel
        masks = np.zeros([h, w, len(boxes)], dtype='uint8')
        # create masks
        for i in range(len(boxes)):
            box = boxes[i]
            row_s, row_e = box[1], box[3]
            col_s, col_e = box[0], box[2]
            masks[row_s:row_e, col_s:col_e, i] = 1
        return masks, np.asarray(classes, dtype='int32')

    def image_reference(self, image_id):
        info = self.image_info[image_id]
        return info['path']


Now that we have the dataloader class, we can load in both training and testing set and visualize a few random images and their masks.

# Create training and validation set
# train set
dataset_train = MicrocontrollerDataset()
dataset_train.load_dataset('Microcontroller Detection/train')
dataset_train.prepare()
print('Train: %d' % len(dataset_train.image_ids))

# test/val set
dataset_val = MicrocontrollerDataset()
dataset_val.load_dataset('Microcontroller Detection/test')
dataset_val.prepare()
print('Test: %d' % len(dataset_val.image_ids))

# Load and display random samples
image_ids = np.random.choice(dataset_train.image_ids, 4)
for image_id in image_ids:
    image = dataset_train.load_image(image_id)
    mask, class_ids = dataset_train.load_mask(image_id)
    visualize.display_top_masks(
        image, mask, class_ids, dataset_train.class_names)
